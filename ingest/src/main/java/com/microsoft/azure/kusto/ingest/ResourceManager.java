package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.KustoClient;
import com.microsoft.azure.kusto.data.KustoResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class ResourceManager {

    public enum ResourceTypes{
        SECURED_READY_FOR_AGGREGATION_QUEUE("SecuredReadyForAggregationQueue"),
        FAILED_INGESTIONS_QUEUE("FailedIngestionsQueue"),
        SUCCESSFUL_INGESTIONS_QUEUE("SuccessfulIngestionsQueue"),
        TEMP_STORAGE("TempStorage"),
        INGESTIONS_STATUS_TABLE("IngestionsStatusTable");

        private String name;

        ResourceTypes(String name) {
            this.name = name;
        }

        String getName(){
            return name;
        }
    }

    private ResourceTypes getResourceTypeByName(String name){
        for (ResourceTypes t : ResourceTypes.values()){
            if (t.name.equalsIgnoreCase(name)){
                return t;
            }
        }
        return null;
    }

    //Ingestion Resources
    private HashMap<ResourceTypes, IngestionResource> ingestionResources;

    //Identity Token
    private String identityToken;

    private KustoClient kustoClient;
    private final long REFRESH_INGESTION_RESOURCES_PERIOD = 1000 * 60 * 60 * 1; // 1 hour
    private Timer timer = new Timer(true);
    private final Logger log = LoggerFactory.getLogger(ResourceManager.class);

    private ReadWriteLock ingestionResourcesLock = new ReentrantReadWriteLock();
    private ReadWriteLock authTokenLock = new ReentrantReadWriteLock();

    public ResourceManager(KustoClient kustoClient) throws Exception {
        this.kustoClient = kustoClient;
        ingestionResources = new HashMap<>();

        TimerTask refreshIngestionResourceValuesTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    refreshIngestionResources();
                } catch (Exception e) {
                    log.error("Error in refreshIngestionResources.", e);
                }
            }
        };

        TimerTask refreshIngestionAuthTokenTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    refreshIngestionAuthToken();
                } catch (Exception e) {
                    log.error("Error in refreshIngestionAuthToken.", e);
                }
            }
        };

        try {
            timer.schedule(refreshIngestionAuthTokenTask, 0, REFRESH_INGESTION_RESOURCES_PERIOD);
            timer.schedule(refreshIngestionResourceValuesTask, 0, REFRESH_INGESTION_RESOURCES_PERIOD);

        } catch (Exception e) {
            log.error("Error in initializing ResourceManager.", e);
            throw e;
        }
    }

    String getIngestionResource(ResourceTypes resourceType) throws Exception {
        if (!ingestionResources.containsKey(resourceType)) {
            // When the value is not available, we need to get the tokens from Kusto (refresh):
            refreshIngestionResources();
            try{
                // If the write lock is locked, then the read will wait here.
                // In other words if the refresh is running yet, then wait until it ends:
                ingestionResourcesLock.readLock().lock();
                if (!ingestionResources.containsKey(resourceType)) {
                    throw new Exception("Unable to get ingestion resources for this type: " + resourceType.getName());
                }
            } finally {
                ingestionResourcesLock.readLock().unlock();
            }
        }
        return ingestionResources.get(resourceType).nextValue();
    }

    String getKustoIdentityToken() throws Exception {
        if (identityToken == null) {
            refreshIngestionAuthToken();
            try{
                authTokenLock.readLock().lock();
                if (identityToken == null) {
                    throw new Exception("Unable to get Identity token");
                }
            } finally{
                authTokenLock.readLock().unlock();
            }
        }
        return identityToken;
    }

    int getSize(ResourceTypes resourceType){
        return ingestionResources.containsKey(resourceType) ? ingestionResources.get(resourceType).getSize() : 0;
    }

    private void addIngestionResource(HashMap<ResourceTypes, IngestionResource> ingestionResources, String key, String value) {
        ResourceTypes resourceType = getResourceTypeByName(key);
        if(!ingestionResources.containsKey(resourceType)){
            ingestionResources.put(resourceType, new IngestionResource(resourceType));
        }
        ingestionResources.get(resourceType).addValue(value);
    }

    private void refreshIngestionResources() throws Exception {
        // Here we use tryLock(): If there is another instance doing the refresh, then just skip it.
        if(ingestionResourcesLock.writeLock().tryLock()){
            try {
                log.info("Refreshing Ingestion Resources");
                KustoResults ingestionResourcesResults = kustoClient.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND);
                if(ingestionResourcesResults != null && ingestionResourcesResults.getValues() != null){
                    HashMap<ResourceTypes, IngestionResource> newIngestionResources = new HashMap<>();
                    // Add the received values to a new IngestionResources map:
                    ingestionResourcesResults.getValues().forEach(pairValues -> {
                        String key = pairValues.get(0);
                        String value = pairValues.get(1);
                        addIngestionResource(newIngestionResources, key, value);
                    });
                    // Replace the class ingestionResources map with the new map:
                    ingestionResources = newIngestionResources;
                }
            }finally {
                ingestionResourcesLock.writeLock().unlock();
            }
        }
    }

    private void refreshIngestionAuthToken() throws Exception {
        if (authTokenLock.writeLock().tryLock()) {
            try {
                log.info("Refreshing Ingestion Auth Token");
                KustoResults identityTokenResult = kustoClient.execute(Commands.KUSTO_IDENTITY_GET_COMMAND);
                if (identityTokenResult != null
                        && identityTokenResult.getValues() != null
                        && identityTokenResult.getValues().size() > 0) {
                    identityToken = identityTokenResult.getValues().get(0).get(identityTokenResult.getIndexByColumnName("AuthorizationContext"));
                }
            } finally {
                authTokenLock.writeLock().unlock();
            }
        }
    }

    private class IngestionResource {
        ResourceTypes type;
        int roundRubinIdx = 0;
        ArrayList<String> valuesList;

        IngestionResource(ResourceTypes resourceType){
            this.type = resourceType;
            valuesList = new ArrayList<>();
        }

        void addValue(String val){
            valuesList.add(val);
        }

        int getSize(){
            return valuesList.size();
        }

        String nextValue(){
            roundRubinIdx = (roundRubinIdx + 1) % valuesList.size();
            return valuesList.get(roundRubinIdx);
        }
    }

}