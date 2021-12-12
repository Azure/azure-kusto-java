package com.microsoft.azure.kusto.ingest;

public class ExponentialRetry {
    private final int maxAttempts;
    private final double sleepBase;
    private final double maxJitter;
    private int retries;

    public ExponentialRetry(int maxAttempts) {
        this(maxAttempts, 1.0, 0.0);
    }

    public ExponentialRetry(int maxAttempts, double sleepBaseSec, double maxJitterSec) {
        this.maxAttempts = maxAttempts;
        this.sleepBase = sleepBaseSec;
        this.maxJitter = maxJitterSec;
        this.retries = 0;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public double getSleepBase() {
        return sleepBase;
    }

    public double getMaxJitter() {
        return maxJitter;
    }

    public double getCurrentSleepMs() {
        return sleepBase * (float) Math.pow(2, retries);
    }

    public int getCurrentAttempt() {
        return retries;
    }

    public boolean shouldRetry() {
        return retries < maxAttempts;
    }

    public void doBackoff(){
        double sleepTime = getCurrentSleepMs();
        double jitter = (float)Math.random() * maxJitter;
        double sleepMs = (sleepTime + jitter) * 1000;
        try {
            Thread.sleep((long)sleepMs);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while sleeping");
        }
        retries++;
    }


}
