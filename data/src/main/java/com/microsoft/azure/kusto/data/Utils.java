package com.microsoft.azure.kusto.data;

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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLException;
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
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

public class Utils {

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
        final EmbeddedChannel decoder = new EmbeddedChannel(ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));

        /*
         * A CompositeByteBuf is used to decode multibyte UTF-8 characters
         * (e.g., 'ä', '€') that are split across network chunks and corrupted during decoding.
         *
         * 1. Instead of decoding small chunks individually, we first decompress and
         * accumulate all bytes into this single logical buffer.
         * 2. It acts as a "zero-copy" wrapper around the multiple, smaller
         * decompressed chunks. It doesn't copy them into one new array but simply
         * holds references to them.
         * 3. By decoding from this composite buffer only once at the very end, we
         * guarantee the entire byte sequence is present, ensuring no characters are split.
         */
        final CompositeByteBuf composite = Unpooled.compositeBuffer();

        return gzipBody
                .doOnNext(byteBuffer -> {
                    ByteBuf in = Unpooled.wrappedBuffer(byteBuffer);
                    decoder.writeInbound(in);
                    ByteBuf decompressed;
                    while ((decompressed = decoder.readInbound()) != null) {
                        composite.addComponent(true, decompressed);
                    }
                })
                .then(Mono.fromCallable(() -> {
                    // This block only executes on successful completion of the Flux.
                    decoder.finish();

                    // Just in case there are any leftover data in the buffer.
                    ByteBuf remaining;
                    while ((remaining = decoder.readInbound()) != null) {
                        composite.addComponent(true, remaining);
                    }

                    // By waiting until all decompressed bytes are collected in the
                    // CompositeByteBuf, we can now decode the entire sequence to a String at once.
                    // This guarantees that no multibyte characters are split during the decoding process.
                    return composite.toString(StandardCharsets.UTF_8);
                }))
                .doFinally(ignore -> {
                    composite.release();
                    decoder.finishAndReleaseAll();
                });
    }

    public static Mono<String> processNonGzipBody(Flux<ByteBuffer> body) {
        final CompositeByteBuf composite = Unpooled.compositeBuffer();

        return body
                .doOnNext(byteBuffer -> composite.addComponent(true, Unpooled.wrappedBuffer(byteBuffer)))
                .then(Mono.fromCallable(() -> composite.toString(StandardCharsets.UTF_8)))
                .doFinally(ignore -> composite.release())
                .switchIfEmpty(Mono.just(StringUtils.EMPTY));
    }

    /**
     * Method responsible for constructing the correct InputStream type based on content encoding header
     *
     * @param response      The response object to determine the content encoding
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
