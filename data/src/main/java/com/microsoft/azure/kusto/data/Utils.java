package com.microsoft.azure.kusto.data;

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
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class Utils {
    private static final int MAX_RETRY_ATTEMPTS = 4;
    private static final long MAX_RETRY_INTERVAL = TimeUnit.SECONDS.toMillis(30);
    private static final long BASE_INTERVAL = TimeUnit.SECONDS.toMillis(2);
    private static final HashSet<Class<? extends IOException>> nonRetriableClasses = new HashSet<Class<? extends IOException>>() {{
        add(InterruptedIOException.class);
        add(UnknownHostException.class);
        add(NoRouteToHostException.class);
        add(SSLException.class);
    }};

    static private final IntervalFunction sleepConfig = IntervalFunction.ofExponentialRandomBackoff(BASE_INTERVAL,
            IntervalFunction.DEFAULT_MULTIPLIER,
            IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR,
            MAX_RETRY_INTERVAL);

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
        long seconds = duration.getSeconds();
        int nanos = duration.getNano();
        long hours = TimeUnit.SECONDS.toHours(seconds) % TimeUnit.DAYS.toHours(1);
        long minutes = TimeUnit.SECONDS.toMinutes(seconds) % TimeUnit.MINUTES.toSeconds(1);
        long secs = seconds % TimeUnit.MINUTES.toSeconds(1);
        long days = TimeUnit.SECONDS.toDays(seconds);
        String positive = String.format(
                "%02d.%02d:%02d:%02d.%.3s",
                days,
                hours,
                minutes,
                secs,
                nanos);

        return seconds < 0 ? "-" + positive : positive;
    }

    public static boolean isRetriableIOException(IOException ex){
        return !nonRetriableClasses.contains(ex.getClass()) &&
                ex.getMessage().contains("timed out");

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

    // added auto bigdecimal deserialization for float and double value, since the bigdecimal values seem to lose precision while auto deserialization to
    // double value
    public static ObjectMapper getObjectMapper() {
        return JsonMapper.builder().configure(MapperFeature.PROPAGATE_TRANSIENT_MARKER, true).addModule(new JavaTimeModule()).build().configure(
                DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true).configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true).setNodeFactory(
                JsonNodeFactory.withExactBigDecimals(true));
    }

}
