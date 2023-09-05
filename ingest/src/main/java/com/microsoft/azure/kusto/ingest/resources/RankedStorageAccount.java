package com.microsoft.azure.kusto.ingest.resources;

import com.microsoft.azure.kusto.ingest.utils.TimeProvider;

import java.util.ArrayDeque;

public class RankedStorageAccount {
    static class Bucket {
        public int successCount;
        public int failureCount;

        public Bucket() {
            this.successCount = 0;
            this.failureCount = 0;
        }
    }

    private final ArrayDeque<Bucket> buckets = new ArrayDeque<>();
    private final String accountName;
    private final int bucketCount;
    private final int bucketDurationInSec;
    private final TimeProvider timeProvider;

    private long lastActionTimestamp;

    public RankedStorageAccount(String accountName, int bucketCount, int bucketDurationInSec, TimeProvider timeProvider) {
        this.accountName = accountName;
        this.bucketCount = bucketCount;
        this.bucketDurationInSec = bucketDurationInSec;
        this.timeProvider = timeProvider;
        lastActionTimestamp = timeProvider.currentTimeMillis();
    }

    public void addResult(boolean success) {
        adjustForTimePassed();
        Bucket lastBucket = buckets.peek();
        assert lastBucket != null;
        if (success) {
            lastBucket.successCount++;
        } else {
            lastBucket.failureCount++;
        }
    }

    private void adjustForTimePassed() {
        if (buckets.isEmpty()) {
            buckets.push(new Bucket());
            return;
        }

        long timePassed = timeProvider.currentTimeMillis() - lastActionTimestamp;
        long bucketsToCreate = timePassed / (bucketDurationInSec * 1000L);
        if (bucketsToCreate >= bucketCount) {
            buckets.clear();
            buckets.push(new Bucket());
            return;
        }

        for (int i = 0; i < bucketsToCreate; i++) {
            buckets.push(new Bucket());
            if (buckets.size() > bucketCount) {
                buckets.poll();
            }
        }
        lastActionTimestamp = timeProvider.currentTimeMillis();
    }

    public double getRank() {
        if (buckets.isEmpty()) {
            return 1;
        }

        int penalty = buckets.size() + 1;
        double rank = 0;
        double totalPenalty = 0;
        for (Bucket bucket : buckets) {
            int total = bucket.successCount + bucket.failureCount;
            if (total == 0) {
                penalty--;
                continue;
            }
            double successRate = (double) bucket.successCount / total;
            rank += successRate * penalty;
            totalPenalty += penalty;
            penalty--;
        }
        return rank / totalPenalty;
    }

    public String getAccountName() {
        return accountName;
    }

}
