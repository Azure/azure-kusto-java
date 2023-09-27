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
    private final int maxNumberOfBuckets;
    private final int bucketDurationInSec;
    private final TimeProvider timeProvider;

    private long lastActionTimestamp;

    public RankedStorageAccount(String accountName, int maxNumberOfBuckets, int bucketDurationInSec, TimeProvider timeProvider) {
        this.accountName = accountName;
        this.maxNumberOfBuckets = maxNumberOfBuckets;
        this.bucketDurationInSec = bucketDurationInSec;
        this.timeProvider = timeProvider;
        lastActionTimestamp = timeProvider.currentTimeMillis();
    }

    public void addResult(boolean success) {
        Bucket lastBucket = adjustForTimePassed();
        if (success) {
            lastBucket.successCount++;
        } else {
            lastBucket.failureCount++;
        }
    }

    private Bucket adjustForTimePassed() {
        if (buckets.isEmpty()) {
            buckets.push(new Bucket());
        }

        long timePassed = timeProvider.currentTimeMillis() - lastActionTimestamp;
        long bucketsToCreate = timePassed / (bucketDurationInSec * 1000L);
        if (bucketsToCreate >= maxNumberOfBuckets) {
            buckets.clear();
            buckets.push(new Bucket());
        }

        for (int i = 0; i < bucketsToCreate; i++) {
            buckets.push(new Bucket());
            if (buckets.size() > maxNumberOfBuckets) {
                buckets.poll();
            }
        }

        lastActionTimestamp = timeProvider.currentTimeMillis();
        return buckets.peek();
    }

    public double getRank() {
        if (buckets.isEmpty()) {
            return 1;
        }

        int bucketWeight = buckets.size() + 1;
        double rank = 0;
        double totalWeight = 0;

        // For each bucket, calculate the success rate ( success / total ) and multiply it by the bucket weight.
        // The older the bucket, the less weight it has. For example, if there are 3 buckets, the oldest bucket will have
        // a weight of 1, the middle bucket will have a weight of 2 and the newest bucket will have a weight of 3.

        for (Bucket bucket : buckets) {
            int total = bucket.successCount + bucket.failureCount;
            if (total == 0) {
                bucketWeight--;
                continue;
            }
            double successRate = (double) bucket.successCount / total;
            rank += successRate * bucketWeight;
            totalWeight += bucketWeight;
            bucketWeight--;
        }
        return rank / totalWeight;
    }

    public String getAccountName() {
        return accountName;
    }

}
