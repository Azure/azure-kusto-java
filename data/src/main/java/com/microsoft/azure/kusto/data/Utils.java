package com.microsoft.azure.kusto.data;

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

import com.azure.core.util.CoreUtils;
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

public class Utils {

    private static final int MAX_RETRY_ATTEMPTS = 4;
    private static final long MAX_RETRY_INTERVAL = TimeUnit.SECONDS.toMillis(30);
    private static final long BASE_INTERVAL = TimeUnit.SECONDS.toMillis(2);

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
                ? processGzipBody(httpResponse.getBody())
                : processNonGzipBody(httpResponse.getBody());
    }

    public static Mono<String> processGzipBody(Flux<ByteBuffer> gzipBody) {
        // To ensure efficiency in terms of performance and memory allocation, a streaming
        // chunked decompression approach is utilized using Netty's EmbeddedChannel. This allows the decompression
        // to occur in chunks, making it more memory-efficient for large payloads, as it prevents the entire
        // compressed stream from being loaded into memory at once (which for example is required by GZIPInputStream for decompression).

        EmbeddedChannel decoder = new EmbeddedChannel(ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));

        return gzipBody
                .reduce(new StringBuilder(), (stringBuilder, byteBuffer) -> {
                    decoder.writeInbound(Unpooled.wrappedBuffer(byteBuffer)); // Write chunk to channel for decompression

                    ByteBuf decompressedByteBuf = decoder.readInbound();
                    if (decompressedByteBuf == null) {
                        return stringBuilder;
                    }

                    String string = decompressedByteBuf.toString(StandardCharsets.UTF_8);
                    decompressedByteBuf.release();

                    if (!decoder.inboundMessages().isEmpty()) { // TODO: remove this when we are sure that only one message exists in the channel
                        throw new IllegalStateException("Expected exactly one message in the channel.");
                    }

                    stringBuilder.append(string);
                    return stringBuilder;
                })
                .map(StringBuilder::toString)
                .doFinally(ignore -> decoder.finishAndReleaseAll());
    }

    private static Mono<String> processNonGzipBody(Flux<ByteBuffer> gzipBody) {
        return gzipBody
                .reduce(new StringBuilder(), (sb, bf) -> {
                    sb.append(StandardCharsets.UTF_8.decode(bf.asReadOnlyBuffer()));
                    return sb;
                })
                .map(StringBuilder::toString);
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

    public static boolean isNullOrEmpty(String str) {
        return str == null || CoreUtils.isNullOrEmpty(str.trim());
    }
}
