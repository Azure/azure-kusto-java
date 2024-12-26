package com.microsoft.azure.kusto.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.SSLException;

import com.azure.core.http.HttpHeader;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpResponse;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.core.lang.Nullable;
import io.github.resilience4j.retry.RetryConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class Utils {

    // Use a custom parallel scheduler for retries to avoid thread starvation in case other services
    // are using the reactor parallel scheduler
    public static final Scheduler ADX_PARALLEL_SCHEDULER = Schedulers.newParallel( // TODO: does that make sense? Should this be done on boundedElastic instead
                                                                                   // or not at all?
            "adx-kusto-parallel",
            Schedulers.DEFAULT_POOL_SIZE,
            true);
    private static final int MAX_RETRY_ATTEMPTS = 4;
    private static final long MAX_RETRY_INTERVAL = TimeUnit.SECONDS.toMillis(30);
    private static final long BASE_INTERVAL = TimeUnit.SECONDS.toMillis(2);
    private static final int DEFAULT_BUFFER_LENGTH = 1024;
    private static final ThreadLocal<StringBuilder> sb = ThreadLocal.withInitial(StringBuilder::new);

    // added auto bigdecimal deserialization for float and double value, since the bigdecimal values seem to lose precision while auto deserialization to
    // double value
    public static ObjectMapper getObjectMapper() {
        return JsonMapper.builder().configure(MapperFeature.PROPAGATE_TRANSIENT_MARKER, true).addModule(new JavaTimeModule()).build().configure(
                DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true).configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true).setNodeFactory(
                        JsonNodeFactory.withExactBigDecimals(true));
    }

    private static final HashSet<Class<? extends IOException>> nonRetriableClasses = new HashSet<Class<? extends IOException>>() {
        {
            add(InterruptedIOException.class);
            add(UnknownHostException.class);
            add(NoRouteToHostException.class);
            add(SSLException.class);
        }
    };

    static private final IntervalFunction sleepConfig = IntervalFunction.ofExponentialRandomBackoff(BASE_INTERVAL,
            IntervalFunction.DEFAULT_MULTIPLIER,
            IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR,
            MAX_RETRY_INTERVAL);

    private Utils() {
        // Hide constructor, as this is a static utility class
    }

    public static String getPackageVersion() {
        try {
            Properties props = new Properties();
            try (InputStream versionFileStream = Utils.class.getResourceAsStream("/app.properties")) {
                props.load(versionFileStream);
                return props.getProperty("version").trim();
            }
        } catch (Exception ignored) {
        }
        return "";
    }

    public static String formatDurationAsTimespan(Duration duration) {
        long durationInSeconds = duration.getSeconds();
        int nanos = duration.getNano();
        long hours = TimeUnit.SECONDS.toHours(durationInSeconds) % TimeUnit.DAYS.toHours(1);
        long minutes = TimeUnit.SECONDS.toMinutes(durationInSeconds) % TimeUnit.HOURS.toMinutes(1);
        long seconds = durationInSeconds % TimeUnit.MINUTES.toSeconds(1);
        long days = TimeUnit.SECONDS.toDays(durationInSeconds);

        String absoluteVal = "";
        if (days != 0) {
            absoluteVal += String.format("%02d.", days);
        }
        absoluteVal += String.format(
                "%02d:%02d:%02d",
                hours,
                minutes,
                seconds);
        if (nanos != 0) {
            absoluteVal += String.format(".%.3s", nanos);
        }

        return durationInSeconds < 0 ? "-" + absoluteVal : absoluteVal;
    }

    public static boolean isRetriableIOException(IOException ex) {
        return !nonRetriableClasses.contains(ex.getClass()) &&
                ex.getMessage() != null && ex.getMessage().contains("timed out");

    }

    public static RetryConfig buildRetryConfig(@Nullable Class<? extends Throwable>... errorClasses) {
        return RetryConfig.custom()
                .maxAttempts(MAX_RETRY_ATTEMPTS)
                .intervalFunction(sleepConfig)
                .retryExceptions(errorClasses)
                .build();
    }

    public static RetryConfig buildRetryConfig(Predicate<Throwable> predicate) {
        return RetryConfig.custom()
                .maxAttempts(MAX_RETRY_ATTEMPTS)
                .intervalFunction(sleepConfig)
                .retryOnException(predicate)
                .build();
    }

    /**
     * Checks if an HTTP response is GZIP compressed.
     *
     * @param response The HTTP response to check
     * @return a boolean indicating if the CONTENT_ENCODING header contains "gzip"
     */
    public static boolean isGzipResponse(HttpResponse response) {
        Optional<HttpHeader> contentEncoding = Optional.ofNullable(response.getHeaders().get(HttpHeaderName.CONTENT_ENCODING));
        return contentEncoding
                .filter(header -> header.getValue().contains("gzip"))
                .isPresent();
    }

    public static Mono<String> getResponseBody(HttpResponse httpResponse) {
        return isGzipResponse(httpResponse)
                ? processGzipBody(httpResponse.getBodyAsInputStream())
                : httpResponse.getBodyAsString(StandardCharsets.UTF_8);
    }

    // TODO: delete all comments

    // (Ohad, Asaf opinions?):
    // This aggregates the entire compressed stream into memory which is required by GZIPInputStream.
    // GZIPInputStream is also blocking
    // Additional memory overhead is added due to ByteArrayOutputStream copies.
    // Same with all methods provided from HttpResponse object such as:
    // response.getBodyAsByteArray(), getBodyAsString() (for non gzip) etc.
    //
    // The most I could test with is 2000000 records (dataset.csv format) and all methods, including this
    // and the one below as well as the methods from HttpResponse, had almost the same query time.
    // I am not sure how representative this is for you guys.
    public static Mono<String> processGzipBody(Mono<InputStream> body) {
        return body
                .map(inputStream -> {
                    try (GZIPInputStream gzipStream = new GZIPInputStream(inputStream);
                            ByteArrayOutputStream output = new ByteArrayOutputStream()) {

                        byte[] buffer = new byte[DEFAULT_BUFFER_LENGTH];
                        int bytesRead;
                        while ((bytesRead = gzipStream.read(buffer)) != -1) {
                            output.write(buffer, 0, bytesRead);
                        }

                        return new String(output.toByteArray(), StandardCharsets.UTF_8);
                    } catch (IOException e) {
                        throw Exceptions.propagate(e);
                    }
                }).subscribeOn(Schedulers.boundedElastic());
    }

    // Alternative an approach to streaming decompression. I had some OOM errors locally though on KustoOperationResult.createFromV2Response
    public static Mono<String> processGzipBody1(Flux<ByteBuffer> gzipBody) {
        // To ensure efficiency in terms of performance and memory allocation, a streaming
        // chunked decompression approach is utilized using Netty's EmbeddedChannel. This allows the decompression
        // to occur in chunks, making it more memory-efficient for large payloads, as it prevents the entire
        // compressed stream from being loaded into memory at once (which is required by GZIPInputStream for decompression).
        // Flux.create() emits decompressed data as it becomes available, while ensuring backpressure.
        // Decompression is offloaded to `boundedElastic()`, which prevents thread blocking and significantly improves performance.

        EmbeddedChannel channel = new EmbeddedChannel(ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));

        return gzipBody
                .flatMap(byteBuffer -> {
                    ByteBuf byteBuf = Unpooled.wrappedBuffer(byteBuffer);
                    channel.writeInbound(byteBuf); // Write chunk to channel for decompression
                    if (byteBuf.refCnt() > 0) {
                        byteBuf.release();
                    }

                    // Read decompressed data from the channel and emit each chunk as a String
                    return Flux.<String>create(sink -> {
                        ByteBuf decompressedByteBuf;
                        while ((decompressedByteBuf = channel.readInbound()) != null) {
                            sink.next(decompressedByteBuf.toString(StandardCharsets.UTF_8));
                            if (decompressedByteBuf.refCnt() > 0) {
                                decompressedByteBuf.release();
                            }
                        }
                        sink.complete();
                    }).doOnError(ignore -> channel.finish());
                }).subscribeOn(Schedulers.boundedElastic())
                .reduce(new StringBuilder(), StringBuilder::append) // TODO: this will cause memory problems right?
                .map(StringBuilder::toString)
                .doFinally(ignore -> channel.finish());
    }

    /**
     * Method responsible for constructing the correct InputStream type based on content encoding header
     *
     * @param response      The response object in order to determine the content encoding
     * @param contentStream The InputStream containing the content
     * @return The correct InputStream type based on content encoding
     */
    public static InputStream resolveInputStream(HttpResponse response, InputStream contentStream) {
        try {
            String contentEncoding = response.getHeaders().get(HttpHeaderName.CONTENT_ENCODING).getValue();
            if (contentEncoding.contains("gzip")) {
                return new GZIPInputStream(contentStream);
            } else if (contentEncoding.contains("deflate")) {
                return new DeflaterInputStream(contentStream);
            }
            return contentStream;
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

}
