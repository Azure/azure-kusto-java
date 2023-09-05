package com.microsoft.azure.kusto.ingest.resources;

import com.microsoft.azure.kusto.ingest.MockTimeProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RankedStorageAccountTest {
    @Test
    public void testGetRankWithNoData() {
        // Rationale: To ensure that the getRank method correctly calculates the rank with no data
        MockTimeProvider timeProvider = new MockTimeProvider(System.currentTimeMillis());
        RankedStorageAccount account = new RankedStorageAccount("testAccount", 5, 10, timeProvider);

        double rank = account.getRank();
        System.out.println(rank);
        assertEquals(1, rank, 0.001);
    }

    @Test
    public void testGetRankWithAllSuccesses() {
        // Rationale: To ensure that the getRank method correctly calculates the rank with all successes
        MockTimeProvider timeProvider = new MockTimeProvider(System.currentTimeMillis());
        RankedStorageAccount account = new RankedStorageAccount("testAccount", 5, 10, timeProvider);

        for (int i = 0; i < 10; i++) {
            account.addResult(true);
        }

        double rank = account.getRank();
        System.out.println(rank);
        assertEquals(1.0, rank, 0.001);
    }

    @Test
    public void testGetRankWithAllFailures() {
        // Rationale: To ensure that the getRank method correctly calculates the rank with all failures
        MockTimeProvider timeProvider = new MockTimeProvider(System.currentTimeMillis());
        RankedStorageAccount account = new RankedStorageAccount("testAccount", 5, 10, timeProvider);

        for (int i = 0; i < 10; i++) {
            account.addResult(false);
        }

        double rank = account.getRank();
        System.out.println(rank);
        assertEquals(0.0, rank, 0.001);
    }

    // Test getRank method with mixed results
    @Test
    public void testGetRankWithMixedResults() {
        // Rationale: To ensure that the getRank method correctly calculates the rank with mixed results
        MockTimeProvider timeProvider = new MockTimeProvider(System.currentTimeMillis());
        RankedStorageAccount account = new RankedStorageAccount("testAccount", 5, 10, timeProvider);

        for (int i = 0; i < 5; i++) {
            account.addResult(true);
        }
        for (int i = 0; i < 5; i++) {
            account.addResult(false);
        }

        double rank = account.getRank();
        System.out.println(rank);
        assertEquals(0.5, rank, 0.001);
    }

    @Test
    public void testNewBucketOverride() {
        // Rationale: To ensure that the new bucket override works as expected
        MockTimeProvider timeProvider = new MockTimeProvider(System.currentTimeMillis());
        RankedStorageAccount account = new RankedStorageAccount("testAccount", 5, 10, timeProvider);

        account.addResult(false);
        account.addResult(false);

        double rank = account.getRank();
        System.out.println(rank);
        assertEquals(0.0, rank, 0.001);

        timeProvider.setCurrentTimeMillis(timeProvider.currentTimeMillis() + 11000);
        account.addResult(true);
        rank = account.getRank();
        System.out.println(rank);
        assertEquals(0.6, rank, 0.001); // it would be 0.333 without the override
    }

    @Test
    public void testSkipBucket() {
        // Rationale: To ensure that the skip bucket works as expected
        MockTimeProvider timeProvider = new MockTimeProvider(System.currentTimeMillis());
        RankedStorageAccount account = new RankedStorageAccount("testAccount", 5, 10, timeProvider);

        account.addResult(false);
        account.addResult(false);

        double rank = account.getRank();
        System.out.println(rank);
        assertEquals(0.0, rank, 0.001);

        timeProvider.setCurrentTimeMillis(timeProvider.currentTimeMillis() + 21000);
        account.addResult(true);
        rank = account.getRank();
        System.out.println(rank);
        assertEquals(0.666, rank, 0.001); // it would be 0.5 without the override
    }

    @Test
    public void testClearBuckets() {
        // Rationale: To ensure that the clear buckets works as expected
        MockTimeProvider timeProvider = new MockTimeProvider(System.currentTimeMillis());
        RankedStorageAccount account = new RankedStorageAccount("testAccount", 5, 10, timeProvider);

        account.addResult(false);
        account.addResult(false);

        double rank = account.getRank();
        System.out.println(rank);
        assertEquals(0.0, rank, 0.001);

        timeProvider.setCurrentTimeMillis(timeProvider.currentTimeMillis() + 51000);
        account.addResult(true);
        rank = account.getRank();
        System.out.println(rank);
        assertEquals(1.0, rank, 0.001); // it would be 0.5 without the override
    }
}
