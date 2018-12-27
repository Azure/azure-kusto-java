package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.Results;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

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

    @BeforeAll
    static void setUp() {
        try {
            Results ingestionResourcesResult = generateIngestionResourcesResult();
            Results ingestionAuthTokenResult = generateIngestionAuthTokenResult();

            when(clientMock.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND))
                    .thenReturn(ingestionResourcesResult);

            when(clientMock.execute(Commands.IDENTITY_GET_COMMAND))
                    .thenReturn(ingestionAuthTokenResult);

            resourceManager = new ResourceManager(clientMock);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void getIdentityToken() {
        try {
            assertEquals(AUTH_TOKEN, resourceManager.getIdentityToken());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void getStorageUri() {
        try {
            String storage;
            HashMap<String,Integer> m = new HashMap();

            for(int i=0; i<10; i++){
                storage = resourceManager.getIngestionResource(ResourceManager.ResourceType.TEMP_STORAGE);
                m.put(storage,m.getOrDefault(storage,0)+1);
            }

            assertEquals(10, m.get(STORAGE_1) + m.get(STORAGE_2));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void getAggregationQueue() {
        try {
            String queueName;
            HashMap<String,Integer> m = new HashMap();

            for(int i=0; i<10; i++){
                queueName = resourceManager.getIngestionResource(ResourceManager.ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE);
                m.put(queueName,m.getOrDefault(queueName,0)+1);
            }

            assertEquals(10, m.get(QUEUE_1) + m.get(QUEUE_2));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Results generateIngestionResourcesResult() {
        HashMap<String, Integer> colNameToIndexMap = new HashMap<>();
        HashMap<String, String> colNameToTypeMap = new HashMap<>();
        ArrayList<ArrayList<String>> valuesList = new ArrayList<>();

        colNameToIndexMap.put("colNameToIndexMap",0);
        colNameToIndexMap.put("StorageRoot",1);

        colNameToTypeMap.put("colNameToIndexMap","String");
        colNameToTypeMap.put("StorageRoot","String");

        valuesList.add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue", QUEUE_1))));
        valuesList.add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue", QUEUE_2))));
        valuesList.add(new ArrayList<>((Arrays.asList("FailedIngestionsQueue","failedQueue"))));
        valuesList.add(new ArrayList<>((Arrays.asList("SuccessfulIngestionsQueue","successQueue"))));
        valuesList.add(new ArrayList<>((Arrays.asList("TempStorage",STORAGE_1))));
        valuesList.add(new ArrayList<>((Arrays.asList("TempStorage",STORAGE_2))));
        valuesList.add(new ArrayList<>((Arrays.asList("IngestionsStatusTable","statusTable"))));

        return new Results(colNameToIndexMap,colNameToTypeMap,valuesList);
    }

    private static Results generateIngestionAuthTokenResult() {
        HashMap<String, Integer> colNameToIndexMap = new HashMap<>();
        HashMap<String, String> colNameToTypeMap = new HashMap<>();
        ArrayList<ArrayList<String>> valuesList = new ArrayList<>();

        colNameToIndexMap.put("AuthorizationContext",0);
        colNameToTypeMap.put("AuthorizationContext","String");
        valuesList.add(new ArrayList<>((Collections.singletonList(AUTH_TOKEN))));

        return new Results(colNameToIndexMap,colNameToTypeMap,valuesList);
    }
}