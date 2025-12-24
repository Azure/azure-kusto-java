// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.storage.blob.BlobContainerAsyncClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import com.microsoft.azure.kusto.data.exceptions.ThrottleException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.resources.QueueWithSas;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ResourceManagerTest {
    private static final Client clientMock = mock(Client.class);
    private static final String AUTH_TOKEN = "AuthenticationToken";
    private static final String STATUS_TABLE = "statusTable";
    private static final String FAILED_QUEUE = "failedQueue";
    private static final String SUCCESS_QUEUE = "successQueue";
    private static List<ContainerWithSas> STORAGES = new ArrayList<>();
    private static final List<QueueWithSas> QUEUES = new ArrayList<>();
    private static final TableWithSas STATUS_TABLE_RES = TestUtils.tableWithSasFromTableName(STATUS_TABLE);
    private static final QueueWithSas FAILED_QUEUE_RES = TestUtils.queueWithSasFromQueueName(FAILED_QUEUE);
    private static final QueueWithSas SUCCESS_QUEUE_RES = TestUtils.queueWithSasFromQueueName(SUCCESS_QUEUE);
    private static ResourceManager resourceManager;
    private static final int ACCOUNTS_COUNT = 10;

    @BeforeAll
    static void setUp() throws DataClientException, DataServiceException {
        // Using answer so that we get a new result set with reset iterator
        when(clientMock.executeMgmtAsync(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                .thenAnswer(invocationOnMock -> Mono.just(generateIngestionResourcesResult()));

        when(clientMock.executeMgmtAsync(Commands.IDENTITY_GET_COMMAND))
                .thenAnswer(invocationOnMock -> Mono.just(generateIngestionAuthTokenResult()));

        setUpStorageResources(0);

        resourceManager = new ResourceManager(clientMock, null);
    }

    static void setUpStorageResources(int startingIndex) {
        STORAGES = new ArrayList<>();
        for (int i = startingIndex; i < startingIndex + ACCOUNTS_COUNT; i++) {
            for (int j = 0; j <= i; j++) { // different number of containers per account
                STORAGES.add(TestUtils.containerWithSasFromAccountNameAndContainerName("storage_" + i, "container_" + i + "_" + j));
                QUEUES.add(TestUtils.queueWithSasFromAccountNameAndQueueName("queue_" + i, "queue_" + i + "_" + j));
            }
        }
    }

    @AfterAll
    static void afterAll() {
        resourceManager.close();
    }

    static KustoOperationResult generateIngestionResourcesResult() throws KustoServiceQueryError, IOException {
        ObjectMapper objectMapper = Utils.getObjectMapper();
        List<List<String>> valuesList = new ArrayList<>();
        for (int i = 0; i < STORAGES.size(); i++) {
            valuesList
                    .add(new ArrayList<>((Arrays.asList("TempStorage", STORAGES.get(i).getAsyncContainer().getBlobContainerUrl() + STORAGES.get(i).getSas()))));
            valuesList
                    .add(new ArrayList<>(
                            (Arrays.asList("SecuredReadyForAggregationQueue", QUEUES.get(i).getAsyncQueue().getQueueUrl() + QUEUES.get(i).getSas()))));
        }
        valuesList.add(new ArrayList<>((Arrays.asList("FailedIngestionsQueue", FAILED_QUEUE_RES.getAsyncQueue().getQueueUrl() + FAILED_QUEUE_RES.getSas()))));
        valuesList.add(
                new ArrayList<>((Arrays.asList("SuccessfulIngestionsQueue", SUCCESS_QUEUE_RES.getAsyncQueue().getQueueUrl() + SUCCESS_QUEUE_RES.getSas()))));
        valuesList.add(new ArrayList<>((Arrays.asList("IngestionsStatusTable", STATUS_TABLE_RES.getTableAsyncClient().getTableEndpoint() + "?sas"))));
        String listAsJson = objectMapper.writeValueAsString(valuesList);
        String response = "{\"Tables\":[{\"TableName\":\"Table_0\",\"Columns\":[{\"ColumnName\":\"ResourceTypeName\"," +
                "\"DataType\":\"String\",\"ColumnType\":\"string\"},{\"ColumnName\":\"StorageRoot\",\"DataType\":" +
                "\"String\",\"ColumnType\":\"string\"}],\"Rows\":"
                + listAsJson + "}]}";

        return new KustoOperationResult(response, "v1");
    }

    static KustoOperationResult generateIngestionAuthTokenResult() throws KustoServiceQueryError, IOException {
        ObjectMapper objectMapper = Utils.getObjectMapper();
        List<List<String>> valuesList = new ArrayList<>();
        valuesList.add(new ArrayList<>((Collections.singletonList(AUTH_TOKEN))));
        String listAsJson = objectMapper.writeValueAsString(valuesList);

        String response = "{\"Tables\":[{\"TableName\":\"Table_0\",\"Columns\":[{\"ColumnName\":\"AuthorizationContext\",\"DataType\":\"String\",\"ColumnType\":\"string\"}],\"Rows\":"
                +
                listAsJson + "}]}";

        return new KustoOperationResult(response, "v1");
    }

    @Test
    void getIdentityToken_ReturnsCorrectToken() throws IngestionServiceException, IngestionClientException {
        assertEquals(AUTH_TOKEN, resourceManager.getIdentityToken());
    }

    @Test
    void getIngestionResource_TempStorage_VerifyRoundRobin() throws IngestionServiceException, IngestionClientException {
        List<ContainerWithSas> storages = resourceManager.getShuffledContainers();
        Pattern pattern = Pattern.compile("container_(\\d+)_(\\d+)");

        int currentRoundRobinIndex = 0;
        Set<Integer> usedAccounts = new HashSet<>();
        for (ContainerWithSas storage : storages) {
            String endpointWithoutSas = storage.getEndpointWithoutSas();
            Matcher matcher = pattern.matcher(endpointWithoutSas);
            assertTrue(matcher.find());
            int accountIndex = Integer.parseInt(matcher.group(1));
            int containerIndex = Integer.parseInt(matcher.group(2));
            if (containerIndex == currentRoundRobinIndex) {
                if (usedAccounts.contains(accountIndex)) {
                    fail("Account " + accountIndex + " was used twice in the same round robin");
                } else {
                    usedAccounts.add(accountIndex);
                }
            } else {
                assertEquals(currentRoundRobinIndex + 1, containerIndex);
                currentRoundRobinIndex = containerIndex;
                usedAccounts.clear();
            }
        }
    }

    @Test
    void getIngestionResource_AggregationQueue_VerifyRoundRobin() throws IngestionServiceException, IngestionClientException {
        List<QueueWithSas> queues = resourceManager.getShuffledQueues();

        Pattern pattern = Pattern.compile("queue_(\\d+)_(\\d+)");

        int currentRoundRobinIndex = 0;
        Set<Integer> usedAccounts = new HashSet<>();
        for (QueueWithSas storage : queues) {
            String endpointWithoutSas = storage.getEndpointWithoutSas();
            Matcher matcher = pattern.matcher(endpointWithoutSas);
            assertTrue(matcher.find());
            int accountIndex = Integer.parseInt(matcher.group(1));
            int containerIndex = Integer.parseInt(matcher.group(2));
            if (containerIndex == currentRoundRobinIndex) {
                if (usedAccounts.contains(accountIndex)) {
                    fail("Account " + accountIndex + " was used twice in the same round robin");
                } else {
                    usedAccounts.add(accountIndex);
                }
            } else {
                assertEquals(currentRoundRobinIndex + 1, containerIndex);
                currentRoundRobinIndex = containerIndex;
                usedAccounts.clear();
            }
        }
    }

    @Test
    void getIngestionResource_StatusTable_ReturnCorrectTable()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                STATUS_TABLE_RES.getUri(),
                resourceManager.getStatusTable().getUri());
    }

    @Test
    void getIngestionResource_FailedIngestionQueue_ReturnCorrectQueue()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                FAILED_QUEUE_RES.getEndpoint(),
                resourceManager.getFailedQueue().getEndpoint());
    }

    @Test
    void getIngestionResource_SuccessfulIngestionQueue_ReturnCorrectQueue()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                SUCCESS_QUEUE_RES.getEndpoint(),
                resourceManager.getSuccessfulQueue().getEndpoint());
    }

    @Test
    void getIngestionResource_WhenNewStorageContainersArrive_ShouldReturnOnlyNewResources()
            throws IngestionServiceException, DataServiceException, DataClientException {
        long waitTime = 1000;
        Client clientMockLocal = mock(Client.class);
        when(clientMockLocal.executeMgmtAsync(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                .thenAnswer(invocationOnMock -> Mono.just(generateIngestionResourcesResult()));

        when(clientMockLocal.executeMgmtAsync(Commands.IDENTITY_GET_COMMAND))
                .thenAnswer(invocationOnMock -> Mono.just(generateIngestionAuthTokenResult()));

        ResourceManager resourceManagerWithLowRefresh = new ResourceManager(clientMockLocal, waitTime, waitTime, null);
        resourceManagerWithLowRefresh.getShuffledContainers();

        setUpStorageResources(10);
        validateStorage(resourceManagerWithLowRefresh.getShuffledContainers());

        resourceManagerWithLowRefresh.close();
    }

    void validateStorage(List<ContainerWithSas> storages) {
        Map<String, List<BlobContainerAsyncClient>> storageByAccount = storages.stream().map(ContainerWithSas::getAsyncContainer)
                .collect(Collectors.groupingBy(BlobContainerAsyncClient::getAccountName));
        assertEquals(ACCOUNTS_COUNT, storageByAccount.size());
    }

    @Test
    void getIngestionResource_WhenStorageFailsToFetch_ReturnGoodContainers()
            throws InterruptedException, IngestionClientException, IngestionServiceException, DataServiceException, DataClientException {
        long waitTime = 200;
        Client clientMockFail = mock(Client.class);
        class Fail {
            public boolean shouldFail;
        }
        final Fail fail = new Fail();
        when(clientMockFail.executeMgmtAsync(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                .thenAnswer(invocationOnMock -> {
                    if (!fail.shouldFail) {
                        return Mono.just(generateIngestionResourcesResult());
                    }
                    return Mono.error(new RuntimeException("Failed something"));
                });
        ResourceManager resourceManagerWithLowRefresh = new ResourceManager(clientMockFail, waitTime, waitTime, null);

        for (int i = 1; i < 10; i++) {
            if (i % 5 == 4) {
                fail.shouldFail = !fail.shouldFail;
            }
            Thread.sleep(i * 200);
            validateStorage(resourceManagerWithLowRefresh.getShuffledContainers());

        }
        resourceManagerWithLowRefresh.close();
    }

    @Test
    void testRetryOnThrottleException_SucceedsAfterRetries() throws Exception {
        Client mockClient = mock(Client.class);
        AtomicInteger attemptCount = new AtomicInteger(0);

        // Fail first 2 attempts with ThrottleException, succeed on 3rd
        when(mockClient.executeMgmtAsync(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                .thenAnswer(invocation -> Mono.defer(() -> {
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt <= 2) {
                        return Mono.error(new ThrottleException("clusterUrl"));
                    }
                    try {
                        return Mono.just(ResourceManagerTest.generateIngestionResourcesResult());
                    } catch (Exception e) {
                        return Mono.error(e);
                    }
                }));
        when(mockClient.executeMgmtAsync(Commands.IDENTITY_GET_COMMAND))
                .thenReturn(Mono.just(ResourceManagerTest.generateIngestionAuthTokenResult()));

        ResourceManager resourceManager = new ResourceManager(mockClient, null);

        resourceManager.refreshIngestionResourcesTask.waitUntilRefreshedAtLeastOnce();

        assertEquals(3, attemptCount.get(), "Should have retried exactly 3 times");
        assertNotNull(resourceManager.getShuffledContainers(), "Should successfully get containers after retries");

        resourceManager.close();
    }
}
