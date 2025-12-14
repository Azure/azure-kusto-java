// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ExponentialRetry;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.ThrottleException;
import com.microsoft.azure.kusto.data.http.HttpClientFactory;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.SupplierTwoExceptions;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.resources.QueueWithSas;
import com.microsoft.azure.kusto.ingest.resources.RankedStorageAccount;
import com.microsoft.azure.kusto.ingest.resources.RankedStorageAccountSet;
import com.microsoft.azure.kusto.ingest.resources.ResourceWithSas;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;
import reactor.util.retry.Retry;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

class ResourceManager implements Closeable, IngestionResourceManager {

    private static final int MAX_RETRY_ATTEMPTS = 4;
    private static final long REFRESH_INGESTION_RESOURCES_PERIOD = TimeUnit.HOURS.toMillis(1);
    private static final long REFRESH_INGESTION_RESOURCES_PERIOD_ON_FAILURE = TimeUnit.MINUTES.toMillis(1);
    private static final long REFRESH_RESULT_POLL_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(15);
    private static final double BASE_INTERVAL_SECS = 2.0;
    private static final double JITTER_FACTOR = 0.5;
    private static final Retry RETRY_CONFIG = new ExponentialRetry(MAX_RETRY_ATTEMPTS, BASE_INTERVAL_SECS, JITTER_FACTOR)
            .retry(Collections.singletonList(ThrottleException.class), null);
    private final Client client;
    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private Timer refreshTasksTimer;
    private final ReadWriteLock ingestionResourcesLock = new ReentrantReadWriteLock();
    private final ReadWriteLock ingestionAuthTokenLock = new ReentrantReadWriteLock();
    private final ReadWriteLock ingestionResourcesSchedulingLock = new ReentrantReadWriteLock();
    private final ReadWriteLock ingestionAuthTokenSchedulingLock = new ReentrantReadWriteLock();
    private final Long defaultRefreshTime;
    private final Long refreshTimeOnFailure;
    private final HttpClient httpClient;
    private RequestRetryOptions queueRequestOptions = null;
    private RankedStorageAccountSet storageAccountSet;
    private String identityToken;
    private IngestionResourceSet ingestionResourceSet;
    protected RefreshIngestionAuthTokenTask refreshIngestionAuthTokenTask;
    protected RefreshIngestionResourcesTask refreshIngestionResourcesTask;

    /// <summary>
    ///
    /// Creates a new instance of the ResourceManager.
    /// This class is not async at its nature therefore it was not implemented as one, a cache is only doing async work
    /// once in a while and most requests should return immediately
    /// </summary>
    public ResourceManager(Client client, long defaultRefreshTime, long refreshTimeOnFailure, @Nullable HttpClient httpClient) {
        this.client = client;
        // Using ctor with client so that the dependency is used
        this.httpClient = httpClient == null
                ? HttpClientFactory.create(HttpClientProperties.builder().build())
                : httpClient;

        // Refresh tasks
        this.refreshTasksTimer = new Timer(true);
        this.defaultRefreshTime = defaultRefreshTime;
        this.refreshTimeOnFailure = refreshTimeOnFailure;
        initRefreshTasks();

        this.storageAccountSet = new RankedStorageAccountSet();
    }

    public ResourceManager(Client client, @Nullable HttpClient httpClient) {
        this(client, REFRESH_INGESTION_RESOURCES_PERIOD, REFRESH_INGESTION_RESOURCES_PERIOD_ON_FAILURE, httpClient);
    }

    @Override
    public void close() {
        refreshTasksTimer.cancel();
        refreshTasksTimer.purge();
        refreshTasksTimer = null;
    }

    abstract static class RefreshResourceTask extends TimerTask {
        protected final BlockingQueue<Boolean> refreshedAtLeastOnce = new LinkedBlockingDeque<>();

        public Boolean waitUntilRefreshedAtLeastOnce() {
            return waitUntilRefreshedAtLeastOnce(REFRESH_RESULT_POLL_TIMEOUT_MILLIS);
        }

        public Boolean waitUntilRefreshedAtLeastOnce(long timeoutMillis) {
            try {
                Boolean refreshedAtLeastOncePollResult = refreshedAtLeastOnce.poll(timeoutMillis, TimeUnit.MILLISECONDS);
                if (refreshedAtLeastOncePollResult != null) {
                    refreshedAtLeastOnce.put(refreshedAtLeastOncePollResult); // Since the poll above removed the indication whether a refresh was done
                    return refreshedAtLeastOncePollResult;
                } else {
                    return null;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
    }

    class RefreshIngestionResourcesTask extends RefreshResourceTask {

        @Override
        public void run() {
            try {
                MonitoredActivity.invoke((SupplierTwoExceptions<Void, IngestionClientException, IngestionServiceException>) () -> {
                    refreshIngestionResources();
                    return null;
                }, "ResourceManager.refreshIngestionResource");
            } catch (Exception e) {
                log.error("Error in refreshIngestionResources: " + e.getMessage(), e);
                scheduleRefreshIngestionResourcesTask(refreshTimeOnFailure);
            }
        }

        private void refreshIngestionResources() throws IngestionClientException, IngestionServiceException {
            // Here we use tryLock(): If there is another instance doing the refresh, then just skip it.
            if (ingestionResourcesLock.writeLock().tryLock()) {
                try {
                    log.info("Refreshing Ingestion Resources");
                    IngestionResourceSet newIngestionResourceSet = new IngestionResourceSet();

                    KustoOperationResult ingestionResourcesResults = client.executeMgmtAsync(Commands.INGESTION_RESOURCES_SHOW_COMMAND)
                            .retryWhen(RETRY_CONFIG)
                            .block();

                    if (ingestionResourcesResults != null) {
                        KustoResultSetTable table = ingestionResourcesResults.getPrimaryResults();
                        // Add the received values to the new ingestion resources
                        while (table.next()) {
                            String resourceTypeName = table.getString(0);
                            String storageUrl = table.getString(1);
                            addIngestionResource(newIngestionResourceSet, resourceTypeName, storageUrl);
                        }
                    }
                    populateStorageAccounts(newIngestionResourceSet);
                    ingestionResourceSet = newIngestionResourceSet;
                    refreshedAtLeastOnce.clear();
                    refreshedAtLeastOnce.put(true);
                    log.info("Refreshing Ingestion Resources Finished");
                } catch (DataServiceException e) {
                    throw new IngestionServiceException(e.getIngestionSource(), "Error refreshing IngestionResources. " + e.getMessage(), e);
                } catch (DataClientException e) {
                    throw new IngestionClientException(e.getIngestionSource(), "Error refreshing IngestionResources. " + e.getMessage(), e);
                } catch (Throwable e) {
                    throw new IngestionClientException(e.getMessage(), e);
                } finally {
                    ingestionResourcesLock.writeLock().unlock();
                }
            }
        }

        private void addIngestionResource(IngestionResourceSet ingestionResourceSet, String resourceTypeName, String storageUrl) throws URISyntaxException {
            ResourceType resourceType = ResourceType.findByResourceTypeName(resourceTypeName);
            switch (resourceType) {
                case TEMP_STORAGE:
                    ingestionResourceSet.containers.addResource(new ContainerWithSas(storageUrl, httpClient));
                    break;
                case INGESTIONS_STATUS_TABLE:
                    ingestionResourceSet.statusTable.addResource(new TableWithSas(storageUrl, httpClient));
                    break;
                case SECURED_READY_FOR_AGGREGATION_QUEUE:
                    ingestionResourceSet.queues.addResource(new QueueWithSas(storageUrl, httpClient, queueRequestOptions));
                    break;
                case SUCCESSFUL_INGESTIONS_QUEUE:
                    ingestionResourceSet.successfulIngestionsQueues.addResource(new QueueWithSas(storageUrl, httpClient, queueRequestOptions));
                    break;
                case FAILED_INGESTIONS_QUEUE:
                    ingestionResourceSet.failedIngestionsQueues.addResource(new QueueWithSas(storageUrl, httpClient, queueRequestOptions));
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + resourceType);
            }
        }

        private void populateStorageAccounts(IngestionResourceSet ingestionResourceSet) {
            RankedStorageAccountSet tempAccount = new RankedStorageAccountSet();
            Stream<? extends ResourceWithSas<?>> queueStream = (ingestionResourceSet.queues == null ? Stream.empty()
                    : ingestionResourceSet.queues.getResourcesList().stream());
            Stream<? extends ResourceWithSas<?>> containerStream = (ingestionResourceSet.containers == null ? Stream.empty()
                    : ingestionResourceSet.containers.getResourcesList().stream());

            Stream.concat(queueStream, containerStream).forEach(resource -> {
                String accountName = resource.getAccountName();
                if (tempAccount.getAccount(accountName) != null) {
                    return;
                }

                RankedStorageAccount previousAccount = storageAccountSet.getAccount(accountName);
                if (previousAccount != null) {
                    tempAccount.addAccount(previousAccount);
                } else {
                    tempAccount.addAccount(accountName);
                }
            });

            storageAccountSet = tempAccount;
        }
    }

    class RefreshIngestionAuthTokenTask extends RefreshResourceTask {
        @Override
        public void run() {
            try {
                MonitoredActivity.invoke((SupplierTwoExceptions<Void, IngestionClientException, IngestionServiceException>) () -> {
                    refreshIngestionAuthToken();
                    return null;
                }, "ResourceManager.refreshIngestionAuthToken");
            } catch (Exception e) {
                log.error("Error in refreshIngestionAuthToken: " + e.getMessage(), e);
                scheduleRefreshIngestionAuthTokenTask(refreshTimeOnFailure);
            }
        }

        private void refreshIngestionAuthToken() throws IngestionClientException, IngestionServiceException {
            if (ingestionAuthTokenLock.writeLock().tryLock()) {
                try {
                    log.info("Refreshing Ingestion Auth Token");

                    KustoOperationResult identityTokenResult = client.executeMgmtAsync(Commands.IDENTITY_GET_COMMAND)
                            .retryWhen(RETRY_CONFIG)
                            .block();

                    if (identityTokenResult != null
                            && identityTokenResult.hasNext()
                            && !identityTokenResult.getResultTables().isEmpty()) {
                        KustoResultSetTable resultTable = identityTokenResult.next();
                        resultTable.next();
                        identityToken = resultTable.getString(0);
                    }
                    refreshedAtLeastOnce.clear();
                    refreshedAtLeastOnce.put(true);
                    log.info("Refreshing Ingestion Auth Token Finished");
                } catch (DataServiceException e) {
                    throw new IngestionServiceException(e.getIngestionSource(), "Error refreshing IngestionAuthToken. " + e.getMessage(), e);
                } catch (DataClientException e) {
                    throw new IngestionClientException(e.getIngestionSource(), "Error refreshing IngestionAuthToken. " + e.getMessage(), e);
                } catch (Throwable e) {
                    throw new IngestionClientException(e.getMessage(), e);
                } finally {
                    ingestionAuthTokenLock.writeLock().unlock();
                }
            }
        }
    }

    private void initRefreshTasks() {
        scheduleRefreshIngestionResourcesTask(0L);
        scheduleRefreshIngestionAuthTokenTask(0L);
    }

    // If we combined these 2 methods, ensuring we're using distinct synchronized locks for both would be inelegant
    private synchronized void scheduleRefreshIngestionResourcesTask(Long delay) {
        if (refreshTasksTimer != null) {
            if (refreshIngestionResourcesTask != null) {
                refreshIngestionResourcesTask.cancel();
            }

            refreshIngestionResourcesTask = new RefreshIngestionResourcesTask();
            refreshTasksTimer.schedule(refreshIngestionResourcesTask, delay, defaultRefreshTime);
        }
    }

    private synchronized void scheduleRefreshIngestionAuthTokenTask(Long delay) {
        if (refreshTasksTimer != null) {
            if (refreshIngestionAuthTokenTask != null) {
                refreshIngestionAuthTokenTask.cancel();
            }

            refreshIngestionAuthTokenTask = new RefreshIngestionAuthTokenTask();
            refreshTasksTimer.schedule(refreshIngestionAuthTokenTask, delay, defaultRefreshTime);
        }
    }

    @Override
    public List<ContainerWithSas> getShuffledContainers() throws IngestionServiceException {
        IngestionResource<ContainerWithSas> containers = getResourceSet(() -> this.ingestionResourceSet.containers);
        return ResourceAlgorithms.getShuffledResources(storageAccountSet.getRankedShuffledAccounts(), containers.getResourcesList());
    }

    public List<QueueWithSas> getShuffledQueues() throws IngestionServiceException {
        IngestionResource<QueueWithSas> queues = getResourceSet(() -> this.ingestionResourceSet.queues);
        return ResourceAlgorithms.getShuffledResources(storageAccountSet.getRankedShuffledAccounts(), queues.getResourcesList());
    }

    public TableWithSas getStatusTable() throws IngestionServiceException {
        return getResource(() -> this.ingestionResourceSet.statusTable);
    }

    public QueueWithSas getFailedQueue() throws IngestionServiceException {
        return getResource(() -> this.ingestionResourceSet.failedIngestionsQueues);
    }

    public QueueWithSas getSuccessfulQueue() throws IngestionServiceException {
        return getResource(() -> this.ingestionResourceSet.successfulIngestionsQueues);
    }

    public String getIdentityToken() throws IngestionServiceException {
        if (identityToken == null) {
            // If this method is called multiple times, don't schedule the task multiple times
            if (ingestionAuthTokenSchedulingLock.writeLock().tryLock()) {
                try {
                    // Scheduling the task with no delay will force it to try now, with its normal retry logic
                    scheduleRefreshIngestionAuthTokenTask(0L);
                } finally {
                    ingestionAuthTokenSchedulingLock.writeLock().unlock();
                }
            }

            Boolean refreshedOnce = refreshIngestionAuthTokenTask.waitUntilRefreshedAtLeastOnce();
            if (identityToken == null) {
                throwNoResultException("Unable to get Identity token", refreshedOnce);
            }
        }

        return identityToken;
    }

    public void setQueueRequestOptions(RequestRetryOptions queueRequestOptions) {
        this.queueRequestOptions = queueRequestOptions;
    }

    private <T> T getResource(Callable<IngestionResource<T>> resourceGetter) throws IngestionServiceException {
        return getResourceSet(resourceGetter).nextResource();
    }

    private <T> IngestionResource<T> getResourceSet(Callable<IngestionResource<T>> resourceGetter) throws IngestionServiceException {
        IngestionResource<T> resource = null;
        try {
            resource = resourceGetter.call();
        } catch (Exception ignore) {
        }

        if (resource == null || resource.empty()) {
            // If this method is called multiple times, don't schedule the task multiple times
            if (ingestionResourcesSchedulingLock.writeLock().tryLock()) {
                try {
                    // Scheduling the task with no delay will force it to try now, with its normal retry logic
                    scheduleRefreshIngestionResourcesTask(0L);
                } finally {
                    ingestionResourcesSchedulingLock.writeLock().unlock();
                }
            }

            // If the write lock is locked (refresh is running), then the read will wait here until it ends
            Boolean refreshedOnce = refreshIngestionResourcesTask.waitUntilRefreshedAtLeastOnce();
            try {
                resource = resourceGetter.call();
            } catch (Exception ignore) {
            }

            if (resource == null || resource.empty()) {
                throwNoResultException("Unable to get ingestion resources for this type: " +
                        (resource == null ? "" : resource.resourceType), refreshedOnce);
            }
        }

        return resource;
    }

    private static void throwNoResultException(String baseMessage, Boolean refreshedOnce) throws IngestionServiceException {
        if (refreshedOnce == null) {
            baseMessage += " because thread checking refresh job timed out or was interrupted";
        } else if (!refreshedOnce) {
            baseMessage += " because refresh job failed";
        }
        throw new IngestionServiceException(baseMessage);
    }

    @Override
    public void reportIngestionResult(ResourceWithSas<?> resource, boolean success) {
        if (storageAccountSet == null) {
            log.warn("StorageAccountSet is null");
            return;
        }
        storageAccountSet.addResultToAccount(resource.getAccountName(), success);
    }

    enum ResourceType {
        SECURED_READY_FOR_AGGREGATION_QUEUE("SecuredReadyForAggregationQueue"),
        FAILED_INGESTIONS_QUEUE("FailedIngestionsQueue"),
        SUCCESSFUL_INGESTIONS_QUEUE("SuccessfulIngestionsQueue"),
        TEMP_STORAGE("TempStorage"),
        INGESTIONS_STATUS_TABLE("IngestionsStatusTable");

        private final String resourceTypeName;

        ResourceType(String resourceTypeName) {
            this.resourceTypeName = resourceTypeName;
        }

        public static ResourceType findByResourceTypeName(String resourceTypeName) {
            for (ResourceType resourceType : values()) {
                if (resourceType.resourceTypeName.equalsIgnoreCase(resourceTypeName)) {
                    return resourceType;
                }
            }
            throw new IllegalArgumentException(resourceTypeName);
        }

        String getResourceTypeName() {
            return resourceTypeName;
        }
    }

    private static class IngestionResource<T> {
        final ResourceType resourceType;
        int roundRobinIdx = 0;
        List<T> resourcesList;

        IngestionResource(ResourceType resourceType) {
            this.resourceType = resourceType;
            resourcesList = new ArrayList<>();
        }

        public List<T> getResourcesList() {
            return resourcesList;
        }

        void addResource(T resource) {
            resourcesList.add(resource);
        }

        T nextResource() {
            roundRobinIdx = (roundRobinIdx + 1) % resourcesList.size();
            return resourcesList.get(roundRobinIdx);
        }

        boolean empty() {
            return this.resourcesList.isEmpty();
        }
    }

    private static class IngestionResourceSet {
        IngestionResource<ContainerWithSas> containers = new IngestionResource<>(ResourceType.TEMP_STORAGE);
        IngestionResource<TableWithSas> statusTable = new IngestionResource<>(ResourceType.INGESTIONS_STATUS_TABLE);
        IngestionResource<QueueWithSas> queues = new IngestionResource<>(ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE);
        IngestionResource<QueueWithSas> successfulIngestionsQueues = new IngestionResource<>(ResourceType.SUCCESSFUL_INGESTIONS_QUEUE);
        IngestionResource<QueueWithSas> failedIngestionsQueues = new IngestionResource<>(ResourceType.FAILED_INGESTIONS_QUEUE);
    }
}
