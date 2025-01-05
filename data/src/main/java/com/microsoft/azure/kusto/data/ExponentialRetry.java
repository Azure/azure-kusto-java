package com.microsoft.azure.kusto.data;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import reactor.util.annotation.Nullable;
import reactor.util.retry.Retry;

public class ExponentialRetry<E1 extends Throwable, E2 extends Throwable> {
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

    // Caller should throw only permanent errors, returning null if a retry is needed
    public <T> T execute(KustoCheckedFunction<Integer, T, E1, E2> function) throws E1, E2 {
        for (int currentAttempt = 0; currentAttempt < maxAttempts; currentAttempt++) {
            log.info("execute: Attempt {}", currentAttempt);

            try {
                T result = function.apply(currentAttempt);
                if (result != null) {
                    return result;
                }
            } catch (Exception e) {
                log.error("execute: Error is permanent, stopping", e);
                throw e;
            }

            double currentSleepSecs = sleepBaseSecs * (float) Math.pow(2, currentAttempt);
            double jitterSecs = (float) Math.random() * maxJitterSecs;
            double sleepMs = (currentSleepSecs + jitterSecs) * 1000;

            log.info("execute: Attempt {} failed, trying again after sleep of {} seconds", currentAttempt, sleepMs / 1000);

            try {
                Thread.sleep((long) sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("execute: Interrupted while sleeping", e);
            }
        }

        return null;
    }

    /**
     * Creates a retry mechanism with exponential backoff and jitter.
     *
     * @param retriableErrorClasses A list of error classes that are considered retriable. If null,
     *                              the method retries for all errors (default behavior).
     * @return A configured {@link Retry} instance
     */
    public Retry retry(@Nullable List<Class<? extends Throwable>> retriableErrorClasses) {
        return Retry.backoff(maxAttempts, Duration.ofSeconds((long) sleepBaseSecs))
                .maxBackoff(Duration.ofSeconds(30))
                .jitter(maxJitterSecs)
                .filter(throwable -> shouldRetry(throwable, retriableErrorClasses))
                .doAfterRetry(retrySignal -> {
                    long currentAttempt = retrySignal.totalRetries() + 1;
                    log.info("Attempt {} failed.", currentAttempt);
                })
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }

    private static boolean shouldRetry(Throwable failure, List<Class<? extends Throwable>> retriableErrorClasses) {
        if (failure instanceof DataServiceException && ((DataServiceException) failure).isPermanent()) {
            log.error("Error is permanent, stopping.", failure);
            return false;
        }

        if (retriableErrorClasses != null) {
            return retriableErrorClasses.stream()
                    .anyMatch(errorClass -> errorClass.isInstance(failure));
        }

        return true;
    }

}
