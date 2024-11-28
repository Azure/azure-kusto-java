package com.microsoft.azure.kusto.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.lang.invoke.MethodHandles;
import java.time.Duration;

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

    public Retry buildRetry() {
        return Retry.backoff(maxAttempts, Duration.ofSeconds((long) sleepBaseSecs))
                .jitter(Math.random() * maxJitterSecs)
                .scheduler(Schedulers.single())
                .doAfterRetry(retrySignal -> {

                    Retry.RetrySignal signalCopy = retrySignal.copy();
                    long currentAttempt = signalCopy.totalRetries();
                    log.info("Current attempt: {}", currentAttempt);
                    if (currentAttempt >= maxAttempts) {
                        log.info("Max retry attempts reached: {}. No more retries will be attempted.", currentAttempt);
                        throw new RuntimeException();
                    }

                    //double currentSleepSecs = sleepBaseSecs * Math.pow(2, currentAttempt);  // Exponential backoff
                    //double jitterSecs = Math.random() * maxJitterSecs;  // Random jitter
                    //sleepBaseSecs = (currentSleepSecs + jitterSecs) * 1000;  // Sleep in milliseconds
                    log.info("Attempt {} failed, retrying after {} ms. Error: {}", currentAttempt, sleepBaseSecs, retrySignal.failure().getMessage());
                });
    }

    public Retry buildRetry1() { //TODO: it applies maxAttempt retries but in parallel threads?!
        return Retry.from(retrySignals ->
                retrySignals.flatMap(retrySignal -> {

                    Retry.RetrySignal signalCopy = retrySignal.copy();
                    long currentAttempt = signalCopy.totalRetries();

                    if (currentAttempt >= maxAttempts) {
                        log.info("Max retry attempts reached: {}. No more retries will be attempted.", currentAttempt);
                        return Mono.error(signalCopy.failure());
                    }

                    double currentSleepSecs = sleepBaseSecs * Math.pow(2, currentAttempt);
                    double jitterSecs = Math.random() * maxJitterSecs;
                    double sleepMs = (currentSleepSecs + jitterSecs) * 1000;

                    log.info("Attempt {} failed, retrying after {} ms. Error: {}", currentAttempt, sleepMs, retrySignal.failure().getMessage());

                    return Mono.delay(Duration.ofMillis((long) sleepMs), Schedulers.single());
                })
        );
    }

}
