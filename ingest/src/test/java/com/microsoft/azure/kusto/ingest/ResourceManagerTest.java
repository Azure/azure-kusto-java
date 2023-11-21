// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.http.HttpPostUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.resources.QueueWithSas;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ResourceManagerTest {
    private static final Client clientMock = mock(Client.class);
    private static final String AUTH_TOKEN = "AuthenticationToken";
    private static final String STATUS_TABLE = "statusTable";
    private static final String FAILED_QUEUE = "failedQueue";
    private static final String SUCCESS_QUEUE = "successQueue";
    private static final List<ContainerWithSas> STORAGES = new ArrayList<>();
    private static final List<QueueWithSas> QUEUES = new ArrayList<>();
    private static final TableWithSas STATUS_TABLE_RES = TestUtils.tableWithSasFromTableName(STATUS_TABLE);
    private static final QueueWithSas FAILED_QUEUE_RES = TestUtils.queueWithSasFromQueueName(FAILED_QUEUE);
    private static final QueueWithSas SUCCESS_QUEUE_RES = TestUtils.queueWithSasFromQueueName(SUCCESS_QUEUE);
    private static ResourceManager resourceManager;

    private static int ACCOUNTS_COUNT = 10;

    @BeforeAll
    static void setUp() throws DataClientException, DataServiceException {
        // Using answer so that we get a new result set with reset iterator
        when(clientMock.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                .thenAnswer(invocationOnMock -> generateIngestionResourcesResult());

        when(clientMock.execute(Commands.IDENTITY_GET_COMMAND))
                .thenAnswer(invocationOnMock -> generateIngestionAuthTokenResult());

        for (int i = 0; i < ACCOUNTS_COUNT; i++) {
            for (int j = 0; j < i; j++) { // different number of containers per account
                STORAGES.add(TestUtils.containerWithSasFromAccountNameAndContainerName("storage_" + i, "container_" + i + "_" + j));
                QUEUES.add(TestUtils.queueWithSasFromAccountNameAndQueueName("queue_" + i, "queue_" + i + "_" + j));
            }
        }

        resourceManager = new ResourceManager(clientMock, null);
    }

    @AfterAll
    static void afterAll() {
        resourceManager.close();
    }

    static KustoOperationResult generateIngestionResourcesResult() throws KustoServiceQueryError, IOException {
        ObjectMapper objectMapper = HttpPostUtils.getObjectMapper();
        List<List<String>> valuesList = new ArrayList<>();
        for (int i = 0; i < STORAGES.size(); i++) {
            valuesList.add(new ArrayList<>((Arrays.asList("TempStorage", STORAGES.get(i).getContainer().getBlobContainerUrl() + STORAGES.get(i).getSas()))));
            valuesList
                    .add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue", QUEUES.get(i).getQueue().getQueueUrl() + QUEUES.get(i).getSas()))));
        }
        valuesList.add(new ArrayList<>((Arrays.asList("FailedIngestionsQueue", FAILED_QUEUE_RES.getQueue().getQueueUrl() + FAILED_QUEUE_RES.getSas()))));
        valuesList.add(new ArrayList<>((Arrays.asList("SuccessfulIngestionsQueue", SUCCESS_QUEUE_RES.getQueue().getQueueUrl() + SUCCESS_QUEUE_RES.getSas()))));
        valuesList.add(new ArrayList<>((Arrays.asList("IngestionsStatusTable", STATUS_TABLE_RES.getTable().getTableEndpoint() + "?sas"))));
        String listAsJson = objectMapper.writeValueAsString(valuesList);
        String response = "{\"Tables\":[{\"TableName\":\"Table_0\",\"Columns\":[{\"ColumnName\":\"ResourceTypeName\"," +
                "\"DataType\":\"String\",\"ColumnType\":\"string\"},{\"ColumnName\":\"StorageRoot\",\"DataType\":" +
                "\"String\",\"ColumnType\":\"string\"}],\"Rows\":"
                + listAsJson + "}]}";

        return new KustoOperationResult(response, "v1");
    }

    static KustoOperationResult generateIngestionAuthTokenResult() throws KustoServiceQueryError, IOException {
        ObjectMapper objectMapper = HttpPostUtils.getObjectMapper();
        List<List<String>> valuesList = new ArrayList<>();
        valuesList.add(new ArrayList<>((Collections.singletonList(AUTH_TOKEN))));
        String listAsJson = objectMapper.writeValueAsString(valuesList);

        String response = "{\"Tables\":[{\"TableName\":\"Table_0\",\"Columns\":[{\"ColumnName\":\"AuthorizationContext\",\"DataType\":\"String\",\"ColumnType\":\"string\"}],\"Rows\":"
                +
                listAsJson + "}]}";

        return new KustoOperationResult(response, "v1");
    }

    @Test
    void GetIdentityToken_ReturnsCorrectToken() throws IngestionServiceException, IngestionClientException {
        assertEquals(AUTH_TOKEN, resourceManager.getIdentityToken());
    }

    @Test
    void GetIngestionResource_TempStorage_VerifyRoundRubin() throws IngestionServiceException, IngestionClientException {
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
    void GetIngestionResource_AggregationQueue_VerifyRoundRubin() throws IngestionServiceException, IngestionClientException {
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
    void GetIngestionResource_StatusTable_ReturnCorrectTable()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                STATUS_TABLE_RES.getUri(),
                resourceManager.getStatusTable().getUri());
    }

    @Test
    void GetIngestionResource_FailedIngestionQueue_ReturnCorrectQueue()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                FAILED_QUEUE_RES.getEndpoint(),
                resourceManager.getFailedQueue().getEndpoint());
    }

    @Test
    void GetIngestionResource_SuccessfulIngestionQueue_ReturnCorrectQueue()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                SUCCESS_QUEUE_RES.getEndpoint(),
                resourceManager.getSuccessfulQueue().getEndpoint());
    }
}
