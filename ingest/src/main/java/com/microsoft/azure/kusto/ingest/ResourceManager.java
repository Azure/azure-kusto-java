package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.KustoClient;
import com.microsoft.azure.kusto.data.KustoResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ResourceManager {

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
    private ConcurrentHashMap<ResourceTypes, IngestionResource> ingestionResources;

    //Identity Token
    private String identityToken;

    private KustoClient kustoClient;
    private final long REFRESH_INGESTION_RESOURCES_PERIOD = 10000;//1000 * 60 * 60 * 1; // 1 hour
    private Timer timer = new Timer(true);
    private final Logger log = LoggerFactory.getLogger(KustoIngestClient.class);

    private ReentrantReadWriteLock ingestionResourcesLock = new ReentrantReadWriteLock();
    private ReentrantReadWriteLock authTokenLock = new ReentrantReadWriteLock();

    public ResourceManager(KustoClient kustoClient) {
        this.kustoClient = kustoClient;
        ingestionResources = new ConcurrentHashMap<>();

        TimerTask refreshIngestionResourceValuesTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    refreshIngestionResources();
                } catch (Exception e) {
                    log.error(String.format("Error in refreshIngestionResources: %s.", e.getMessage()), e);
                }
            }
        };

        TimerTask refreshIngestionAuthTokenTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    refreshIngestionAuthToken();
                } catch (Exception e) {
                    log.error(String.format("Error in refreshIngestionAuthToken: %s.", e.getMessage()), e);
                }
            }
        };

        try {
            timer.schedule(refreshIngestionAuthTokenTask, 0, REFRESH_INGESTION_RESOURCES_PERIOD);
            timer.schedule(refreshIngestionResourceValuesTask, 0, REFRESH_INGESTION_RESOURCES_PERIOD);

        } catch (Exception e) {
            log.error("Error in initializing ResourceManager: {}.", e.getMessage(), e);
            throw e;
        }
    }

    public void clean() {
        ingestionResources.clear();
    }

    public String getKustoIdentityToken() throws Exception {
        authTokenLock.readLock().lock();
        try {
            if (identityToken == null) {
                refreshIngestionAuthToken();
                if (identityToken == null) {
                    throw new Exception("Unable to get Identity token");
                }
            }
        } finally {
            authTokenLock.readLock().unlock();
        }

        return identityToken;
    }

    public String getIngestionResource(ResourceTypes resourceType) throws Exception {
        String ingestionResource;
        ingestionResourcesLock.readLock().lock();
        try {
            if (!ingestionResources.containsKey(resourceType)) {
                refreshIngestionResources();
                if (!ingestionResources.containsKey(resourceType)) {
                    throw new Exception("Unable to get ingestion resources for this type: " + resourceType.getName());
                }
            }

            ingestionResource = ingestionResources.get(resourceType).nextValue();
        } finally {
            ingestionResourcesLock.readLock().unlock();
        }

        return ingestionResource;
    }

    public int getSize(ResourceTypes resourceType){
        return ingestionResources.containsKey(resourceType) ? ingestionResources.get(resourceType).getSize() : 0;
    }

    private void addValue(String key, String value) {
        ResourceTypes resourceType = getResourceTypeByName(key);
        if(!ingestionResources.containsKey(resourceType)){
            ingestionResources.put(resourceType, new IngestionResource(resourceType));
        }
        ingestionResources.get(resourceType).addValue(value);
    }

    private void refreshIngestionResources() throws Exception {
        log.info("Refreshing Ingestion Resources");
        ingestionResourcesLock.writeLock().lock();
        try {
            KustoResults ingestionResourcesResults = kustoClient.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND);
            ArrayList<ArrayList<String>> values = ingestionResourcesResults.getValues();

            clean();

            values.forEach(pairValues -> {
                String key = pairValues.get(0);
                String value = pairValues.get(1);
                addValue(key, value);
            });
        } finally {
            ingestionResourcesLock.writeLock().unlock();
        }
    }

    private void refreshIngestionAuthToken() throws Exception {
        log.info("Refreshing Ingestion Auth Token");
        ingestionResourcesLock.writeLock().lock();
        try {
            KustoResults identityTokenResult = kustoClient.execute(Commands.KUSTO_IDENTITY_GET_COMMAND);
            identityToken = identityTokenResult.getValues().get(0).get(identityTokenResult.getIndexByColumnName("AuthorizationContext"));
        } finally {
            ingestionResourcesLock.writeLock().unlock();
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