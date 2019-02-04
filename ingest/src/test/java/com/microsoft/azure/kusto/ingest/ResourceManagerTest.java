package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.Results;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    static void setUp() throws DataClientException, DataServiceException {
        when(clientMock.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                .thenReturn(generateIngestionResourcesResult());

        when(clientMock.execute(Commands.IDENTITY_GET_COMMAND))
                .thenReturn(generateIngestionAuthTokenResult());

        resourceManager = new ResourceManager(clientMock);
    }

    @Test
    void getIdentityTokenReturnsCorrectToken() throws IngestionServiceException, IngestionClientException {
        assertEquals(AUTH_TOKEN, resourceManager.getIdentityToken());
    }

    @Test
    void getStorageUriVerifyRoundRubin() throws IngestionServiceException, IngestionClientException {
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
    void getAggregationQueueVerifyRoundRubin() throws IngestionServiceException, IngestionClientException {
        String queueName;
        HashMap<String, Integer> m = new HashMap();

        for (int i = 0; i < 10; i++) {
            queueName = resourceManager
                    .getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE);
            m.put(queueName, m.getOrDefault(queueName, 0) + 1);
        }

        assertEquals(5, (int) m.get(QUEUE_1));
        assertEquals(5, (int) m.get(QUEUE_2));
    }

    @Test
    void getStatusTableReturnsCorrectToken() throws IngestionServiceException, IngestionClientException {
        assertEquals(
                STATUS_TABLE,
                resourceManager.getIngestionResource(ResourceManager.ResourceType.INGESTIONS_STATUS_TABLE));
    }

    @Test
    void getFailedQueueReturnsCorrectToken() throws IngestionServiceException, IngestionClientException {
        assertEquals(
                FAILED_QUEUE,
                resourceManager.getIngestionResource(ResourceManager.ResourceType.FAILED_INGESTIONS_QUEUE));
    }

    @Test
    void getSuccessQueueReturnsCorrectToken() throws IngestionServiceException, IngestionClientException {
        assertEquals(
                SUCCESS_QUEUE,
                resourceManager.getIngestionResource(ResourceManager.ResourceType.SUCCESSFUL_INGESTIONS_QUEUE));
    }

    private static Results generateIngestionResourcesResult() {
        HashMap<String, Integer> colNameToIndexMap = new HashMap<>();
        HashMap<String, String> colNameToTypeMap = new HashMap<>();
        ArrayList<ArrayList<String>> valuesList = new ArrayList<>();

        colNameToIndexMap.put("colNameToIndexMap", 0);
        colNameToIndexMap.put("StorageRoot", 1);

        colNameToTypeMap.put("colNameToIndexMap", "String");
        colNameToTypeMap.put("StorageRoot", "String");

        valuesList.add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue", QUEUE_1))));
        valuesList.add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue", QUEUE_2))));
        valuesList.add(new ArrayList<>((Arrays.asList("FailedIngestionsQueue", FAILED_QUEUE))));
        valuesList.add(new ArrayList<>((Arrays.asList("SuccessfulIngestionsQueue", SUCCESS_QUEUE))));
        valuesList.add(new ArrayList<>((Arrays.asList("TempStorage", STORAGE_1))));
        valuesList.add(new ArrayList<>((Arrays.asList("TempStorage", STORAGE_2))));
        valuesList.add(new ArrayList<>((Arrays.asList("IngestionsStatusTable", STATUS_TABLE))));

        return new Results(colNameToIndexMap, colNameToTypeMap, valuesList);
    }

    private static Results generateIngestionAuthTokenResult() {
        HashMap<String, Integer> colNameToIndexMap = new HashMap<>();
        HashMap<String, String> colNameToTypeMap = new HashMap<>();
        ArrayList<ArrayList<String>> valuesList = new ArrayList<>();

        colNameToIndexMap.put("AuthorizationContext", 0);
        colNameToTypeMap.put("AuthorizationContext", "String");
        valuesList.add(new ArrayList<>((Collections.singletonList(AUTH_TOKEN))));

        return new Results(colNameToIndexMap, colNameToTypeMap, valuesList);
    }
}