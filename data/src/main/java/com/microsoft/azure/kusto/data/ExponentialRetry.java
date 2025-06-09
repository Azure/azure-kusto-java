package com.microsoft.azure.kusto.data;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase;

import reactor.util.annotation.Nullable;
import reactor.util.retry.Retry;

public class ExponentialRetry {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final int maxAttempts;
    double sleepBaseSecs;
    double maxJitterSecs;

    public ExponentialRetry(int maxAttempts) {
        this.maxAttempts = maxAttempts;
        this.sleepBaseSecs = 1.0;
        this.maxJitterSecs = 1.0;
    }

    public ExponentialRetry(int maxAttempts, double sleepBaseSecs, double maxJitterSecs) {
        this.maxAttempts = maxAttempts;
        this.sleepBaseSecs = sleepBaseSecs;
        this.maxJitterSecs = maxJitterSecs;
    }

    public ExponentialRetry(ExponentialRetry other) {
        this.maxAttempts = other.maxAttempts;
        this.sleepBaseSecs = other.sleepBaseSecs;
        this.maxJitterSecs = other.maxJitterSecs;
    }

    /**
     * Creates a retry mechanism with exponential backoff and jitter.
     *
     * @param retriableErrorClasses A list of error classes that are considered retriable. If null,
     *                              the method does not retry.
    * @param filter A filter to use. Default is retrying retriable errors.
     * @return A configured {@link Retry} instance
     */
    public Retry retry(@Nullable List<Class<? extends Throwable>> retriableErrorClasses,
            @Nullable Predicate<? super Throwable> filter) {
        if (retriableErrorClasses != null && filter != null) {
            throw new IllegalArgumentException("Cannot specify both retriableErrorClasses and filter");
        }

        Predicate<? super Throwable> filterToUse = filter == null ? throwable -> shouldRetry(throwable, retriableErrorClasses) : filter;
        return Retry.backoff(maxAttempts, Duration.ofSeconds((long) sleepBaseSecs))
                .maxBackoff(Duration.ofSeconds(30))
                .jitter(maxJitterSecs)
                .filter(filterToUse)
                .doAfterRetry(retrySignal -> {
                    long currentAttempt = retrySignal.totalRetries() + 1;
                    log.info("Attempt {} failed.", currentAttempt);
                })
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }

    private static boolean shouldRetry(Throwable failure, List<Class<? extends Throwable>> retriableErrorClasses) {
        if (failure instanceof KustoDataExceptionBase) {
            return !((KustoDataExceptionBase) failure).isPermanent();
        }

        if (retriableErrorClasses != null) {
            return retriableErrorClasses.stream()
                    .anyMatch(errorClass -> errorClass.isInstance(failure));
        }

        return false;
    }
}
