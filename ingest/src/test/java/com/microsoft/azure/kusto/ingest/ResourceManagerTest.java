package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceError;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class ResourceManagerTest {

    private static ResourceManager resourceManager;
    private static Client clientMock = mock(Client.class);

    private static final String QUEUE_1 = "queue1";
    private static final String QUEUE_2 = "queue2";
    private static final String STORAGE_1 = "storage1";
    private static final String STORAGE_2 = "storage2";
    private static final String AUTH_TOKEN = "AuthenticationToken";
    private static final String STATUS_TABLE = "statusTable";
    private static final String FAILED_QUEUE = "failedQueue";
    private static final String SUCCESS_QUEUE = "successQueue";

    @BeforeAll
    static void setUp() throws DataClientException, DataServiceException, JSONException, KustoServiceError, IOException {
        when(clientMock.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                .thenReturn(generateIngestionResourcesResult());

        when(clientMock.execute(Commands.IDENTITY_GET_COMMAND))
                .thenReturn(generateIngestionAuthTokenResult());

        resourceManager = null;
    }

    @Test
    void GetIdentityToken_ReturnsCorrectToken() throws IngestionServiceException, IngestionClientException {
        assertEquals(AUTH_TOKEN, resourceManager.getIdentityToken());
    }

    @Test
    void GetIngestionResource_TempStorage_VerifyRoundRubin()
            throws IngestionServiceException, IngestionClientException {
        List<String> availableStorages = new ArrayList<>(Arrays.asList(STORAGE_1, STORAGE_2));

        String storage = resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE);
        int lastIndex = availableStorages.indexOf(storage);

        for (int i = 0; i < 10; i++) {
            storage = resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE);
            int currIdx = availableStorages.indexOf(storage);
            assertEquals((lastIndex + 1) % availableStorages.size(), currIdx);
            lastIndex = currIdx;
        }
    }

    @Test
    void GetIngestionResource_AggregationQueue_VerifyRoundRubin()
            throws IngestionServiceException, IngestionClientException {
        List<String> availableQueues = new ArrayList<>(Arrays.asList(QUEUE_1, QUEUE_2));

        String queue = resourceManager
                .getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE);
        int lastIndex = availableQueues.indexOf(queue);

        for (int i = 0; i < 10; i++) {
            queue = resourceManager
                    .getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE);
            int currIdx = availableQueues.indexOf(queue);
            assertEquals((lastIndex + 1) % availableQueues.size(), currIdx);
            lastIndex = currIdx;
        }
    }

    @Test
    void GetIngestionResource_StatusTable_ReturnCorrectTable()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                STATUS_TABLE,
                resourceManager.getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE));
    }

    @Test
    void GetIngestionResource_FailedIngestionQueue_ReturnCorrectQueue()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                FAILED_QUEUE,
                resourceManager.getIngestionResource(ResourceManager.ResourceType.FAILED_INGESTIONS_QUEUE));
    }

    @Test
    void GetIngestionResource_SuccessfulIngestionQueue_ReturnCorrectQueue()
            throws IngestionServiceException, IngestionClientException {
        assertEquals(
                SUCCESS_QUEUE,
                resourceManager.getIngestionResource(ResourceManager.ResourceType.SUCCESSFUL_INGESTIONS_QUEUE));
    }

    @Test
    void TimerTest() throws DataClientException, DataServiceException, InterruptedException, KustoServiceError, IOException {
        Client clientMock = mock(Client.class);
        final ArrayList<Date> refreshTimestamps = new ArrayList<>();
        when(clientMock.execute(Commands.IDENTITY_GET_COMMAND))
                .thenReturn(generateIngestionAuthTokenResult());
        when(clientMock.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND)).then((Answer) invocationOnMock -> {
            refreshTimestamps.add((new Date()));
            if(refreshTimestamps.size() != 1){
                throw new Exception();
            }

            return generateIngestionResourcesResult();
        });

        ResourceManager resourceManager = new ResourceManager(clientMock, 1000L,500L);
        Thread.sleep(10);
        assertEquals(refreshTimestamps.size() , 1);
        Thread.sleep(1000);
        assertEquals(refreshTimestamps.size() , 2);
        Thread.sleep(500);
        assertEquals(refreshTimestamps.size() , 3);
        resourceManager.close();
    }

    private static KustoOperationResult generateIngestionResourcesResult() throws JSONException, KustoServiceError, IOException {
        ArrayList<ArrayList<String>> valuesList = new ArrayList<>();
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

    private static KustoOperationResult generateIngestionAuthTokenResult() throws JSONException, KustoServiceError, IOException {
        ArrayList<ArrayList<String>> valuesList = new ArrayList<>();
        valuesList.add(new ArrayList<>((Collections.singletonList(AUTH_TOKEN))));
        String listAsJson = new ObjectMapper().writeValueAsString(valuesList);

        String response = "{\"Tables\":[{\"TableName\":\"Table_0\",\"Columns\":[{\"ColumnName\":\"AuthorizationContext\",\"DataType\":\"String\",\"ColumnType\":\"string\"}],\"Rows\":" +
                listAsJson + "}]}";

        return new KustoOperationResult(response, "v1");
    }
}