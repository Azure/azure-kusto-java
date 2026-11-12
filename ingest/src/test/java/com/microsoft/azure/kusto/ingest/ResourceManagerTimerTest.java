package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.BaseClient;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.microsoft.azure.kusto.ingest.ResourceManagerTest.generateIngestionAuthTokenResult;
import static com.microsoft.azure.kusto.ingest.ResourceManagerTest.generateIngestionResourcesResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ResourceManagerTimerTest {

    private static final long REFRESH_TIMEOUT_MILLIS = 5000;
    private static final long SCHEDULING_TOLERANCE_MILLIS = 100;

    @Test
    void timerTest() throws DataClientException, DataServiceException, KustoServiceQueryError, IOException, InterruptedException {
        Client mockedClient = mock(Client.class);
        BlockingQueue<Long> refreshTimestamps = new LinkedBlockingQueue<>();
        AtomicInteger refreshCount = new AtomicInteger();
        when(mockedClient.executeMgmtAsync(Commands.IDENTITY_GET_COMMAND))
                .thenReturn(Mono.just(generateIngestionAuthTokenResult()));
        when(mockedClient.executeMgmtAsync(Commands.INGESTION_RESOURCES_SHOW_COMMAND)).thenAnswer(invocation -> {
            refreshTimestamps.add(System.nanoTime());
            if (refreshCount.incrementAndGet() == 2) {
                return Mono.error(new Exception());
            }

            return Mono.just(generateIngestionResourcesResult());
        });

        try (ResourceManager resourceManager = new ResourceManager(mockedClient, 1000L, 500L, null)) {
            ResourceManager.RefreshIngestionResourcesTask initialTask = resourceManager.refreshIngestionResourcesTask;
            long firstRefresh = awaitRefresh(mockedClient, refreshTimestamps, 1);
            assertEquals(Boolean.TRUE, initialTask.waitUntilRefreshedAtLeastOnce(REFRESH_TIMEOUT_MILLIS));
            assertEquals(Boolean.TRUE, resourceManager.refreshIngestionAuthTokenTask.waitUntilRefreshedAtLeastOnce(REFRESH_TIMEOUT_MILLIS));
            long secondRefresh = awaitRefresh(mockedClient, refreshTimestamps, 2);
            long thirdRefresh = awaitRefresh(mockedClient, refreshTimestamps, 3);

            ResourceManager.RefreshIngestionResourcesTask recoveryTask = resourceManager.refreshIngestionResourcesTask;
            assertNotSame(initialTask, recoveryTask, "A failed refresh must schedule a new task");
            assertEquals(Boolean.TRUE, recoveryTask.waitUntilRefreshedAtLeastOnce(REFRESH_TIMEOUT_MILLIS),
                    "The refresh must succeed after the second attempt fails");
            assertRefreshInterval(firstRefresh, secondRefresh, 1000L);
            assertRefreshInterval(secondRefresh, thirdRefresh, 500L);
        }
    }

    @Test
    void timerTestFailureGettingResources() throws DataClientException, DataServiceException, InterruptedException {
        Client mockedClient = mock(Client.class);
        BlockingQueue<Long> refreshTimestamps = new LinkedBlockingQueue<>();
        when(mockedClient.executeMgmtAsync(Commands.IDENTITY_GET_COMMAND))
                .thenReturn(Mono.error(
                        new RuntimeException(BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", null, new Exception(), "error"))));
        when(mockedClient.executeMgmtAsync(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                .thenAnswer(invocation -> {
                    refreshTimestamps.add(System.nanoTime());
                    return Mono.error(
                            new RuntimeException(BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", null, new Exception(), "error")));
                });

        try (ResourceManager resourceManager = new ResourceManager(mockedClient, 1000L, 500L, null)) {
            long firstRefresh = awaitRefresh(mockedClient, refreshTimestamps, 1);
            assertTrue(resourceManager.refreshIngestionResourcesTask.refreshedAtLeastOnce.isEmpty());
            assertTrue(resourceManager.refreshIngestionAuthTokenTask.refreshedAtLeastOnce.isEmpty());
            long secondRefresh = awaitRefresh(mockedClient, refreshTimestamps, 2);
            long thirdRefresh = awaitRefresh(mockedClient, refreshTimestamps, 3);
            assertRefreshInterval(firstRefresh, secondRefresh, 500L);
            assertRefreshInterval(secondRefresh, thirdRefresh, 500L);
            assertNull(resourceManager.refreshIngestionResourcesTask.waitUntilRefreshedAtLeastOnce(1));
            assertNull(resourceManager.refreshIngestionAuthTokenTask.waitUntilRefreshedAtLeastOnce(1));
        }
    }

    private static long awaitRefresh(Client mockedClient, BlockingQueue<Long> refreshTimestamps, int expectedCount)
            throws InterruptedException, DataClientException, DataServiceException {
        Long timestamp = refreshTimestamps.poll(REFRESH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        assertNotNull(timestamp, "Expected refresh attempt " + expectedCount + " within " + REFRESH_TIMEOUT_MILLIS + " ms");
        verify(mockedClient, atLeast(expectedCount)).executeMgmtAsync(Commands.INGESTION_RESOURCES_SHOW_COMMAND);
        return timestamp;
    }

    private static void assertRefreshInterval(long previousRefresh, long currentRefresh, long expectedDelayMillis) {
        long intervalMillis = TimeUnit.NANOSECONDS.toMillis(currentRefresh - previousRefresh);
        long minimumDelayMillis = expectedDelayMillis - SCHEDULING_TOLERANCE_MILLIS;
        assertTrue(intervalMillis >= minimumDelayMillis && intervalMillis <= REFRESH_TIMEOUT_MILLIS,
                () -> "Expected refresh interval between " + minimumDelayMillis + " and " + REFRESH_TIMEOUT_MILLIS + " ms, but was " + intervalMillis + " ms");
    }
}
