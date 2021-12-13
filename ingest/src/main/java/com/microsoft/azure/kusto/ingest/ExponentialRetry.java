package com.microsoft.azure.kusto.ingest;

public class ExponentialRetry {
    private final int maxAttempts;
    private final double sleepBaseSecs;
    private final double maxJitterSecs;
    private int currentAttempt;

    public ExponentialRetry(int maxAttempts) {
        this(maxAttempts, 1.0, 0.0);
    }

    public ExponentialRetry(int maxAttempts, double sleepBaseSecs, double maxJitterSecs) {
        this.maxAttempts = maxAttempts;
        this.sleepBaseSecs = sleepBaseSecs;
        this.maxJitterSecs = maxJitterSecs;
        this.currentAttempt = 0;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public double getSleepBaseSecs() {
        return sleepBaseSecs;
    }

    public double getMaxJitterSecs() {
        return maxJitterSecs;
    }

    public double getCurrentSleepSecs() {
        return sleepBaseSecs * (float) Math.pow(2, currentAttempt);
    }

    public int getCurrentAttempt() {
        return currentAttempt;
    }

    public boolean shouldTry() {
        return currentAttempt < maxAttempts;
    }

    public void doBackoff(){
        if (!shouldTry()) {
            throw new IllegalStateException("Max attempts exceeded");
        }

        double sleepTime = getCurrentSleepSecs();
        double jitter = (float)Math.random() * maxJitterSecs;
        double sleepMs = (sleepTime + jitter) * 1000;
        try {
            Thread.sleep((long)sleepMs);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while sleeping");
        }
        currentAttempt++;
    }


}
