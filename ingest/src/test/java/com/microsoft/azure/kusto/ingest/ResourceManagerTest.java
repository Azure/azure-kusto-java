// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.utils.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.utils.QueueWithSas;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;
import org.json.JSONException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ResourceManagerTest {
    private static ResourceManager resourceManager;
    private static final Client clientMock = mock(Client.class);
    private static final String QUEUE_1 = "queue1";
    private static final String QUEUE_2 = "queue2";
    private static final String STORAGE_1 = "storage1";
    private static final String STORAGE_2 = "storage2";
    private static final String AUTH_TOKEN = "AuthenticationToken";
    private static final String STATUS_TABLE = "statusTable";
    private static final String FAILED_QUEUE = "failedQueue";
    private static final String SUCCESS_QUEUE = "successQueue";
    private static final QueueWithSas QUEUE_1_RES = TestUtils.queueWithSasFromQueueName(QUEUE_1);
    private static final QueueWithSas QUEUE_2_RES = TestUtils.queueWithSasFromQueueName(QUEUE_2);
    private static final ContainerWithSas STORAGE_1_RES = TestUtils.containerWithSasFromContainerName(STORAGE_1);
    private static final ContainerWithSas STORAGE_2_RES = TestUtils.containerWithSasFromContainerName(STORAGE_2);
    private static final TableWithSas STATUS_TABLE_RES = TestUtils.tableWithSasFromTableName(STATUS_TABLE);
    private static final QueueWithSas FAILED_QUEUE_RES = TestUtils.queueWithSasFromQueueName(FAILED_QUEUE);
    private static final QueueWithSas SUCCESS_QUEUE_RES = TestUtils.queueWithSasFromQueueName(SUCCESS_QUEUE);

    @BeforeAll
    static void setUp() throws DataClientException, DataServiceException, JSONException, KustoServiceQueryError, IOException {
        when(clientMock.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                .thenReturn(generateIngestionResourcesResult());

        when(clientMock.execute(Commands.IDENTITY_GET_COMMAND))
                .thenReturn(generateIngestionAuthTokenResult());

        resourceManager = new ResourceManager(clientMock, null);
    }

    @AfterAll
    static void afterAll() {
        resourceManager.close();
    }

    @Test
    void GetIdentityToken_ReturnsCorrectToken() throws IngestionServiceException, IngestionClientException {
        assertEquals(AUTH_TOKEN, resourceManager.getIdentityToken());
    }

    @Test
    void GetIngestionResource_TempStorage_VerifyRoundRubin()
            throws IngestionServiceException, IngestionClientException {
        List<String> availableStorages = new ArrayList<>(Arrays.asList(
                STORAGE_1_RES.getEndpoint(), STORAGE_2_RES.getEndpoint()));

        ContainerWithSas storage = resourceManager.getTempStorage();
        int lastIndex = availableStorages.indexOf(storage.getEndpoint());

        for (int i = 0; i < 10; i++) {
            storage = resourceManager.getTempStorage();
            int currIdx = availableStorages.indexOf(storage.getEndpoint());
            assertEquals((lastIndex + 1) % availableStorages.size(), currIdx);
            lastIndex = currIdx;
        }
    }

    @Test
    void GetIngestionResource_AggregationQueue_VerifyRoundRubin()
            throws IngestionServiceException, IngestionClientException {
        List<String> availableQueues = new ArrayList<>(Arrays.asList(
                TestUtils.queueWithSasFromQueueName(QUEUE_1).getEndpoint(), TestUtils.queueWithSasFromQueueName(QUEUE_2).getEndpoint()));

        QueueWithSas queue = resourceManager
                .getQueue();
        int lastIndex = availableQueues.indexOf(queue.getEndpoint());

        for (int i = 0; i < 10; i++) {
            queue = resourceManager
                    .getQueue();
            int currIdx = availableQueues.indexOf(queue.getEndpoint());
            assertEquals((lastIndex + 1) % availableQueues.size(), currIdx);
            lastIndex = currIdx;
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

    static KustoOperationResult generateIngestionResourcesResult() throws JSONException, KustoServiceQueryError, IOException {
        List<List<String>> valuesList = new ArrayList<>();
        valuesList.add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue", QUEUE_1_RES.getQueue().getQueueUrl() + QUEUE_1_RES.getSas()))));
        valuesList.add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue", QUEUE_2_RES.getQueue().getQueueUrl() + QUEUE_2_RES.getSas()))));
        valuesList.add(new ArrayList<>((Arrays.asList("FailedIngestionsQueue", FAILED_QUEUE_RES.getQueue().getQueueUrl() + FAILED_QUEUE_RES.getSas()))));
        valuesList.add(new ArrayList<>((Arrays.asList("SuccessfulIngestionsQueue", SUCCESS_QUEUE_RES.getQueue().getQueueUrl() + SUCCESS_QUEUE_RES.getSas()))));
        valuesList.add(new ArrayList<>((Arrays.asList("TempStorage", STORAGE_1_RES.getContainer().getBlobContainerUrl() + STORAGE_1_RES.getSas()))));
        valuesList.add(new ArrayList<>((Arrays.asList("TempStorage", STORAGE_2_RES.getContainer().getBlobContainerUrl() + STORAGE_2_RES.getSas()))));
        valuesList.add(new ArrayList<>((Arrays.asList("IngestionsStatusTable", STATUS_TABLE_RES.getTable().getTableEndpoint() + "?sas"))));
        String listAsJson = new ObjectMapper().writeValueAsString(valuesList);
        String response = "{\"Tables\":[{\"TableName\":\"Table_0\",\"Columns\":[{\"ColumnName\":\"ResourceTypeName\"," +
                "\"DataType\":\"String\",\"ColumnType\":\"string\"},{\"ColumnName\":\"StorageRoot\",\"DataType\":" +
                "\"String\",\"ColumnType\":\"string\"}],\"Rows\":"
                + listAsJson + "}]}";

        return new KustoOperationResult(response, "v1");
    }

    static KustoOperationResult generateIngestionAuthTokenResult() throws JSONException, KustoServiceQueryError, IOException {
        List<List<String>> valuesList = new ArrayList<>();
        valuesList.add(new ArrayList<>((Collections.singletonList(AUTH_TOKEN))));
        String listAsJson = new ObjectMapper().writeValueAsString(valuesList);

        String response = "{\"Tables\":[{\"TableName\":\"Table_0\",\"Columns\":[{\"ColumnName\":\"AuthorizationContext\",\"DataType\":\"String\",\"ColumnType\":\"string\"}],\"Rows\":"
                +
                listAsJson + "}]}";

        return new KustoOperationResult(response, "v1");
    }
}
