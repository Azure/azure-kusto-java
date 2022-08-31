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
import org.json.JSONException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ResourceManagerTest {
    private static ResourceManager resourceManager;
    private static final Client clientMock = mock(Client.class);
    private static final String QUEUE_1 = "https://acc.queue.core.windows.net/queue1?sas";
    private static final String QUEUE_2 = "https://acc.queue.core.windows.net/queue2?sas";
    private static final String STORAGE_1 = "https://acc.blob.core.windows.net/container1?sas";
    private static final String STORAGE_2 = "https://acc.blob.core.windows.net/container2?sas";
    private static final String AUTH_TOKEN = "AuthenticationToken";
    private static final String STATUS_TABLE = "https://acc.table.core.windows.net/statusTable?sass";
    private static final String FAILED_QUEUE = "https://acc.queue.core.windows.net/failedQueue?sas";
    private static final String SUCCESS_QUEUE = "https://acc.queue.core.windows.net/successQueue?sas";

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
        List<String> availableStorages = new ArrayList<>(Arrays.asList(STORAGE_1, STORAGE_2));

        ContainerWithSas storage = resourceManager.getTempStorage();
        int lastIndex = availableStorages.indexOf(storage + storage.getSas());

        for (int i = 0; i < 10; i++) {
            storage = resourceManager.getTempStorage();
            int currIdx = availableStorages.indexOf(storage.getContainer() + storage.getSas());
            assertEquals((lastIndex + 1) % availableStorages.size(), currIdx);
            lastIndex = currIdx;
        }
    }

    @Test
    void GetIngestionResource_AggregationQueue_VerifyRoundRubin()
            throws IngestionServiceException, IngestionClientException {
        List<String> availableQueues = new ArrayList<>(Arrays.asList(QUEUE_1, QUEUE_2));

        QueueWithSas queue = resourceManager
                .getQueue();
        int lastIndex = availableQueues.indexOf(queue.getQueue().getQueueUrl() + queue.getSas());

        for (int i = 0; i < 10; i++) {
            queue = resourceManager
                    .getQueue();
            int currIdx = availableQueues.indexOf(queue.getQueue().getQueueUrl() + queue.getSas());
            assertEquals((lastIndex + 1) % availableQueues.size(), currIdx);
            lastIndex = currIdx;
        }
    }

    @Test
    void GetIngestionResource_StatusTable_ReturnCorrectTable()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                TestUtils.tableWithSasFromTableName(STATUS_TABLE),
                resourceManager.getStatusTable());
    }

    @Test
    void GetIngestionResource_FailedIngestionQueue_ReturnCorrectQueue()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                TestUtils.containerWithSasFromBlobName(FAILED_QUEUE),
                resourceManager.getFailedQueues());
    }

    @Test
    void GetIngestionResource_SuccessfulIngestionQueue_ReturnCorrectQueue()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                TestUtils.queueWithSasFromQueueName(SUCCESS_QUEUE),
                resourceManager.getSuccessfullQueues());
    }

    static KustoOperationResult generateIngestionResourcesResult() throws JSONException, KustoServiceQueryError, IOException {
        List<List<String>> valuesList = new ArrayList<>();
        valuesList.add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue", QUEUE_1))));
        valuesList.add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue", QUEUE_2))));
        valuesList.add(new ArrayList<>((Arrays.asList("FailedIngestionsQueue", FAILED_QUEUE))));
        valuesList.add(new ArrayList<>((Arrays.asList("SuccessfulIngestionsQueue", SUCCESS_QUEUE))));
        valuesList.add(new ArrayList<>((Arrays.asList("TempStorage", STORAGE_1))));
        valuesList.add(new ArrayList<>((Arrays.asList("TempStorage", STORAGE_2))));
        valuesList.add(new ArrayList<>((Arrays.asList("IngestionsStatusTable", STATUS_TABLE))));
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
