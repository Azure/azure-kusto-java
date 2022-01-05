package com.microsoft.azure.kusto.ingest;

public class ExponentialRetry {
    private final int maxAttempts;
    double sleepBaseSecs;
    double maxJitterSecs;
    private int currentAttempt;

    public ExponentialRetry(int maxAttempts) {
        this.maxAttempts = maxAttempts;
        this.sleepBaseSecs = 1.0;
        this.maxJitterSecs = 1.0;
        this.currentAttempt = 0;
    }

    public ExponentialRetry(ExponentialRetry other) {
        this.maxAttempts = other.maxAttempts;
        this.sleepBaseSecs = other.sleepBaseSecs;
        this.maxJitterSecs = other.maxJitterSecs;
    }

    private double getCurrentSleepSecs() {
        return sleepBaseSecs * (float) Math.pow(2, currentAttempt);
    }

    public int getCurrentAttempt() {
        return currentAttempt;
    }

    public String nextTimeToSleep() {
        return String.format("%s +~ %s seconds", getCurrentSleepSecs(), maxJitterSecs);
    }

    public boolean doBackoff() {
        double jitterSeconds = (float) Math.random() * maxJitterSecs;
        double sleepMs = (getCurrentSleepSecs() + jitterSeconds) * 1000;
        try {
            Thread.sleep((long) sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while sleeping", e);
        }
        currentAttempt++;

        return currentAttempt < maxAttempts;
    }


}
