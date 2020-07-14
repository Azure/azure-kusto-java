// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class ResourceManager implements Closeable {

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

    private ConcurrentHashMap<ResourceType, IngestionResource> ingestionResources;
    private String identityToken;
    private Client client;
    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Timer timer;
    private ReadWriteLock ingestionResourcesLock = new ReentrantReadWriteLock();
    private ReadWriteLock authTokenLock = new ReentrantReadWriteLock();
    private static final long REFRESH_INGESTION_RESOURCES_PERIOD = 1000 * 60 * 60; // 1 hour
    private static final long REFRESH_INGESTION_RESOURCES_PERIOD_ON_FAILURE = 1000 * 60 * 15; // 15 minutes
    private Long defaultRefreshTime;
    private Long refreshTimeOnFailure;
    private static final String SERVICE_TYPE_COLUMN_NAME = "ServiceType";

    ResourceManager(Client client, long defaultRefreshTime, long refreshTimeOnFailure) {
        this.defaultRefreshTime = defaultRefreshTime;
        this.refreshTimeOnFailure = refreshTimeOnFailure;
        this.client = client;
        timer = new Timer(true);
        init();
    }

    ResourceManager(Client client) {
        this(client, REFRESH_INGESTION_RESOURCES_PERIOD, REFRESH_INGESTION_RESOURCES_PERIOD_ON_FAILURE);
    }

    @Override
    public void close() {
        timer.cancel();
        timer.purge();
    }

    private void init() {
        ingestionResources = new ConcurrentHashMap<>();
        class RefreshIngestionResourcesTask extends TimerTask {
            @Override
            public void run() {
                try {
                    refreshIngestionResources();
                    timer.schedule(new RefreshIngestionResourcesTask(), defaultRefreshTime);
                } catch (Exception e) {
                    log.error("Error in refreshIngestionResources.", e);
                    timer.schedule(new RefreshIngestionResourcesTask(), refreshTimeOnFailure);
                }
            }
        }

        class RefreshIngestionAuthTokenTask extends TimerTask {
            @Override
            public void run() {
                try {
                    refreshIngestionAuthToken();
                    timer.schedule(new RefreshIngestionAuthTokenTask(), defaultRefreshTime);
                } catch (Exception e) {
                    log.error("Error in refreshIngestionAuthToken.", e);
                    timer.schedule(new RefreshIngestionAuthTokenTask(), refreshTimeOnFailure);
                }
            }
        }

        timer.schedule(new RefreshIngestionAuthTokenTask(), 0);
        timer.schedule(new RefreshIngestionResourcesTask(), 0);
    }

    String getIngestionResource(ResourceType resourceType) throws IngestionServiceException, IngestionClientException {
        if (!ingestionResources.containsKey(resourceType)) {
            // When the value is not available, we need to get the tokens from Kusto (refresh):
            refreshIngestionResources();
            try {
                // If the write lock is locked, then the read will wait here.
                // In other words if the refresh is running yet, then wait until it ends:
                ingestionResourcesLock.readLock().lock();
                if (!ingestionResources.containsKey(resourceType)) {
                    throw new IngestionServiceException("Unable to get ingestion resources for this type: " + resourceType.getName());
                }
            } finally {
                ingestionResourcesLock.readLock().unlock();
            }
        }
        return ingestionResources.get(resourceType).nextValue();
    }

    String getIdentityToken() throws IngestionServiceException, IngestionClientException {
        if (identityToken == null) {
            refreshIngestionAuthToken();
            try {
                authTokenLock.readLock().lock();
                if (identityToken == null) {
                    throw new IngestionServiceException("Unable to get Identity token");
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

    private void refreshIngestionResources() throws IngestionClientException, IngestionServiceException {
        // Here we use tryLock(): If there is another instance doing the refresh, then just skip it.
        // TODO we might want to force refresh if coming from ingestion flow, same with identityToken
        if (ingestionResourcesLock.writeLock().tryLock()) {
            try {
                log.info("Refreshing Ingestion Resources");
                KustoOperationResult ingestionResourcesResults = client.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND);
                if (ingestionResourcesResults != null && ingestionResourcesResults.hasNext()) {
                    HashMap<ResourceType, IngestionResource> newIngestionResources = new HashMap<>();
                    KustoResultSetTable table = ingestionResourcesResults.next();

                    // Add the received values to a new IngestionResources map:
                    while (table.next()) {
                        String key = table.getString(0);
                        String value = table.getString(1);
                        addIngestionResource(newIngestionResources, key, value);
                    }
                    // Replace the values in the ingestionResources map with the values in the new map:
                    putIngestionResourceValues(ingestionResources, newIngestionResources);
                }
            } catch (DataServiceException e) {
                throw new IngestionServiceException(e.getIngestionSource(), "Error refreshing IngestionResources", e);
            } catch (DataClientException e) {
                throw new IngestionClientException(e.getIngestionSource(), "Error refreshing IngestionResources", e);
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

    private void refreshIngestionAuthToken() throws IngestionClientException, IngestionServiceException {
        if (authTokenLock.writeLock().tryLock()) {
            try {
                log.info("Refreshing Ingestion Auth Token");
                KustoOperationResult identityTokenResult = client.execute(Commands.IDENTITY_GET_COMMAND);
                if (identityTokenResult != null
                        && identityTokenResult.hasNext()
                        && identityTokenResult.getResultTables().size() > 0) {
                    KustoResultSetTable resultTable = identityTokenResult.next();
                    resultTable.next();

                    identityToken = resultTable.getString(0);
                }
            } catch (DataServiceException e) {
                throw new IngestionServiceException(e.getIngestionSource(), "Error refreshing IngestionAuthToken", e);
            } catch (DataClientException e) {
                throw new IngestionClientException(e.getIngestionSource(), "Error refreshing IngestionAuthToken", e);
            } finally {
                authTokenLock.writeLock().unlock();
            }
        }
    }

    protected String getServiceType() throws IngestionServiceException, IngestionClientException {
        log.info("Getting version to determine endpoint's ServiceType");
        try {
            KustoOperationResult versionResult = client.execute(Commands.VERSION_SHOW_COMMAND);
            if (versionResult != null && versionResult.hasNext() && versionResult.getResultTables().size() > 0) {
                KustoResultSetTable resultTable = versionResult.next();
                resultTable.next();
                return resultTable.getString(SERVICE_TYPE_COLUMN_NAME);
            }
        } catch (DataServiceException e) {
            throw new IngestionServiceException(e.getIngestionSource(), "Error getting version", e);
        } catch (DataClientException e) {
            throw new IngestionClientException(e.getIngestionSource(), "Error getting version", e);
        }
        throw new IngestionServiceException("Show version command didn't return any records");
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

        String nextValue() {
            roundRubinIdx = (roundRubinIdx + 1) % valuesList.size();
            return valuesList.get(roundRubinIdx);
        }
    }
}