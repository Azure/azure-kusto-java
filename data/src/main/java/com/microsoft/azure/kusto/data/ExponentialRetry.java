package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.lang.invoke.MethodHandles;
import java.time.Duration;

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

    public Retry retry() {
        return Retry.from(retrySignals -> retrySignals.flatMap(retrySignal -> {

            Retry.RetrySignal signalCopy = retrySignal.copy();
            long currentAttempt = signalCopy.totalRetries();
            log.info("Retry attempt {}.", currentAttempt);

            Throwable failure = signalCopy.failure();
            if (failure instanceof DataServiceException && ((DataServiceException) failure).isPermanent()) {
                log.error("Error is permanent, stopping.", failure);
                return Mono.error(failure);
            }

            if (currentAttempt >= maxAttempts) {
                log.info("Max retry attempts reached: {}.", currentAttempt);
                return Mono.error(failure);
            }

            double currentSleepSecs = sleepBaseSecs * (float) Math.pow(2, currentAttempt);
            double jitterSecs = (float) Math.random() * maxJitterSecs;
            double sleepMs = (currentSleepSecs + jitterSecs) * 1000;

            log.info("Attempt {} failed, trying again after sleep of {} seconds.", currentAttempt, sleepMs / 1000);

            // Each retry can occur on a different thread
            return Mono.delay(Duration.ofMillis((long) sleepMs));
        }));
    }

}
