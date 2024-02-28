package com.microsoft.azure.kusto.data;

import com.azure.core.implementation.StringBuilderWriter;
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

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.Writer;
import java.io.InputStreamReader;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;

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

    // TODO Copied from apache IoUtils - should we take it back ? don't recall why removed
    public static String gzipedInputToString(InputStream in) {
        try (GZIPInputStream gz = new GZIPInputStream(in)) {
            StringBuilder stringBuilder = new StringBuilder();
            try (StringBuilderWriter sw = new StringBuilderWriter(stringBuilder)) {
                copy(gz, sw);
                return stringBuilder.toString();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int copy(final InputStream input, final Writer writer)
            throws IOException {
        final InputStreamReader reader = new InputStreamReader(input);
        final char[] buffer = new char[1024];
        int count = 0;
        int n;
        while (-1 != (n = reader.read(buffer))) {
            writer.write(buffer, 0, n);
            count += n;
        }
        return count;
    }
}
