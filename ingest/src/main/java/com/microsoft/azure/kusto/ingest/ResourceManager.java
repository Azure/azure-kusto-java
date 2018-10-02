package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class ResourceManager {

    public enum ResourceType {
        SECURED_READY_FOR_AGGREGATION_QUEUE("SecuredReadyForAggregationQueue"),
        FAILED_INGESTIONS_QUEUE("FailedIngestionsQueue"),
        SUCCESSFUL_INGESTIONS_QUEUE("SuccessfulIngestionsQueue"),
        TEMP_STORAGE("TempStorage"),
        INGESTIONS_STATUS_TABLE("IngestionsStatusTable");

        private String name;

        ResourceType(String name) {
            this.name = name;
        }

        String getName() {
            return name;
        }
    }

    private ResourceType getResourceTypeByName(String name) {
        for (ResourceType t : ResourceType.values()) {
            if (t.name.equalsIgnoreCase(name)) {
                return t;
            }
        }
        return null;
    }

    //Ingestion Resources
    private ConcurrentHashMap<ResourceType, IngestionResource> ingestionResources;

    //Identity Token
    private String identityToken;

    private Client client;
    private final Logger log = LoggerFactory.getLogger(ResourceManager.class);

    private ReadWriteLock ingestionResourcesLock = new ReentrantReadWriteLock();
    private ReadWriteLock authTokenLock = new ReentrantReadWriteLock();

    ResourceManager(Client client) throws Exception {
        this.client = client;
        ingestionResources = new ConcurrentHashMap<>();

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
            Timer timer = new Timer(true);
            long REFRESH_INGESTION_RESOURCES_PERIOD = 1000 * 60 * 60; // 1 hour
            timer.schedule(refreshIngestionAuthTokenTask, 0, REFRESH_INGESTION_RESOURCES_PERIOD);
            timer.schedule(refreshIngestionResourceValuesTask, 0, REFRESH_INGESTION_RESOURCES_PERIOD);

        } catch (Exception e) {
            log.error("Error in initializing ResourceManager.", e);
            throw e;
        }
    }

    String getIngestionResource(ResourceType resourceType) throws Exception {
        if (!ingestionResources.containsKey(resourceType)) {
            // When the value is not available, we need to get the tokens from Kusto (refresh):
            refreshIngestionResources();
            try {
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

    String getIdentityToken() throws Exception {
        if (identityToken == null) {
            refreshIngestionAuthToken();
            try {
                authTokenLock.readLock().lock();
                if (identityToken == null) {
                    throw new Exception("Unable to get Identity token");
                }
            } finally {
                authTokenLock.readLock().unlock();
            }
        }
        return identityToken;
    }

    private void addIngestionResource(HashMap<ResourceType, IngestionResource> ingestionResources, String key, String value) {
        ResourceType resourceType = getResourceTypeByName(key);
        if (!ingestionResources.containsKey(resourceType)) {
            ingestionResources.put(resourceType, new IngestionResource(resourceType));
        }
        ingestionResources.get(resourceType).addValue(value);
    }

    private void refreshIngestionResources() throws Exception {
        // Here we use tryLock(): If there is another instance doing the refresh, then just skip it.
        if (ingestionResourcesLock.writeLock().tryLock()) {
            try {
                log.info("Refreshing Ingestion Resources");
                Results ingestionResourcesResults = client.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND);
                if (ingestionResourcesResults != null && ingestionResourcesResults.getValues() != null) {
                    HashMap<ResourceType, IngestionResource> newIngestionResources = new HashMap<>();
                    // Add the received values to a new IngestionResources map:
                    ingestionResourcesResults.getValues().forEach(pairValues -> {
                        String key = pairValues.get(0);
                        String value = pairValues.get(1);
                        addIngestionResource(newIngestionResources, key, value);
                    });
                    // Replace the values in the ingestionResources map with the values in the new map:
                    putIngestionResourceValues(ingestionResources, newIngestionResources);
                }
            } finally {
                ingestionResourcesLock.writeLock().unlock();
            }
        }
    }

    private void putIngestionResourceValues(ConcurrentHashMap<ResourceType, IngestionResource> ingestionResources, HashMap<ResourceType, IngestionResource> newIngestionResources) {
        // Update the values in the original resources map:
        newIngestionResources.keySet().forEach(
                k -> ingestionResources.put(k, newIngestionResources.get(k))
        );

        // Remove the key-value pairs that are not existing in the new resources map:
        ingestionResources.keySet().forEach(k -> {
            if (!newIngestionResources.containsKey(k)) {
                ingestionResources.remove(k);
            }
        });
    }

    private void refreshIngestionAuthToken() throws Exception {
        if (authTokenLock.writeLock().tryLock()) {
            try {
                log.info("Refreshing Ingestion Auth Token");
                Results identityTokenResult = client.execute(Commands.KUSTO_IDENTITY_GET_COMMAND);
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
        ResourceType type;
        int roundRubinIdx = 0;
        ArrayList<String> valuesList;

        IngestionResource(ResourceType resourceType) {
            this.type = resourceType;
            valuesList = new ArrayList<>();
        }

        void addValue(String val) {
            valuesList.add(val);
        }

        int getSize() {
            return valuesList.size();
        }

        String nextValue() {
            roundRubinIdx = (roundRubinIdx + 1) % valuesList.size();
            return valuesList.get(roundRubinIdx);
        }
    }

}