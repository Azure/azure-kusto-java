// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientProvider;
import com.azure.core.util.HttpClientOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.ThrottleException;
import com.microsoft.azure.kusto.data.http.HttpClientFactory;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.SupplierTwoExceptions;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.resources.RankedStorageAccount;
import com.microsoft.azure.kusto.ingest.resources.RankedStorageAccountSet;
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.resources.QueueWithSas;
import com.microsoft.azure.kusto.ingest.resources.ResourceWithSas;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.vavr.CheckedFunction0;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

class ResourceManager implements Closeable, IngestionResourceManager {
    private static final long REFRESH_INGESTION_RESOURCES_PERIOD = TimeUnit.HOURS.toMillis(1);
    private static final long REFRESH_INGESTION_RESOURCES_PERIOD_ON_FAILURE = TimeUnit.MINUTES.toMillis(1);
    private static final long REFRESH_WITH_RETRIES_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(5);
    public static final int UPLOAD_TIMEOUT_MINUTES = 10;
    private final Client client;
    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private Timer timer;
    private final ReadWriteLock ingestionResourcesLock = new ReentrantReadWriteLock();
    private final ReadWriteLock ingestionAuthTokenLock = new ReentrantReadWriteLock();
    private final Condition ingestionResourcesRefreshedOnce = ingestionResourcesLock.writeLock().newCondition();
    private final Condition ingestionAuthTokenRefreshedOnce = ingestionAuthTokenLock.writeLock().newCondition();
    private final Long defaultRefreshTime;
    private final Long refreshTimeOnFailure;
    private final HttpClient httpClient;
    private final RetryConfig retryConfig;
    private RequestRetryOptions queueRequestOptions = null;
    private RankedStorageAccountSet storageAccountSet;
    private String identityToken;
    private IngestionResourceSet ingestionResourceSet;
    protected RefreshIngestionAuthTokenTask refreshIngestionAuthTokenTask;
    protected RefreshIngestionResourcesTask refreshIngestionResourcesTask;

    public ResourceManager(Client client, long defaultRefreshTime, long refreshTimeOnFailure, @Nullable HttpClient httpClient) {
        this.defaultRefreshTime = defaultRefreshTime;
        this.refreshTimeOnFailure = refreshTimeOnFailure;
        this.client = client;
        timer = new Timer(true);
        // Using ctor with client so that the dependency is used
        this.httpClient = httpClient == null
                ? HttpClientFactory.create(HttpClientProperties.builder().timeout(Duration.ofMinutes(UPLOAD_TIMEOUT_MINUTES)).build())
                : httpClient;
        retryConfig = Utils.buildRetryConfig(ThrottleException.class);
        storageAccountSet = new RankedStorageAccountSet();
        init();
    }

    public ResourceManager(Client client, @Nullable HttpClient httpClient) {
        this(client, REFRESH_INGESTION_RESOURCES_PERIOD, REFRESH_INGESTION_RESOURCES_PERIOD_ON_FAILURE, httpClient);
    }

    @Override
    public void close() {
        timer.cancel();
        timer.purge();
        timer = null;
        try {
            client.close();
        } catch (IOException e) {
            log.error("Couldn't close client: " + e.getMessage(), e);
        }
    }

    class RefreshIngestionResourcesTask extends TimerTask {
        private boolean refreshedAtLeastOnce = false;

        public boolean isRefreshedAtLeastOnce() {
            return refreshedAtLeastOnce;
        }

        @Override
        public void run() {
            try {
                MonitoredActivity.invoke((SupplierTwoExceptions<Void, IngestionClientException, IngestionServiceException>) () -> {
                    refreshIngestionResources();
                    return null;
                }, "ResourceManager.refreshIngestionResource");
            } catch (Exception e) {
                log.error("Error in refreshIngestionResources. " + e.getMessage(), e);
                scheduleRefreshIngestionResourcesTask(refreshTimeOnFailure);
            }
        }

        private void refreshIngestionResources() throws IngestionClientException, IngestionServiceException {
            // Here we use tryLock(): If there is another instance doing the refresh, then just skip it.
            if (ingestionResourcesLock.writeLock().tryLock()) {
                try {
                    log.info("Refreshing Ingestion Resources");
                    IngestionResourceSet newIngestionResourceSet = new IngestionResourceSet();
                    Retry retry = Retry.of("get ingestion resources", retryConfig);
                    CheckedFunction0<KustoOperationResult> retryExecute = Retry.decorateCheckedSupplier(retry,
                            () -> client.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND));
                    KustoOperationResult ingestionResourcesResults = retryExecute.apply();
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
                    refreshedAtLeastOnce = true;
                    ingestionResourcesRefreshedOnce.signalAll();
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

    class RefreshIngestionAuthTokenTask extends TimerTask {
        private boolean refreshedAtLeastOnce = false;

        public boolean isRefreshedAtLeastOnce() {
            return refreshedAtLeastOnce;
        }

        @Override
        public void run() {
            try {
                MonitoredActivity.invoke((SupplierTwoExceptions<Void, IngestionClientException, IngestionServiceException>) () -> {
                    refreshIngestionAuthToken();
                    return null;
                }, "ResourceManager.refreshIngestionAuthToken");
            } catch (Exception e) {
                log.error("Error in refreshIngestionAuthToken. " + e.getMessage(), e);
                scheduleRefreshIngestionAuthTokenTask(refreshTimeOnFailure);
            }
        }

        private void refreshIngestionAuthToken() throws IngestionClientException, IngestionServiceException {
            if (ingestionAuthTokenLock.writeLock().tryLock()) {
                try {
                    log.info("Refreshing Ingestion Auth Token");
                    Retry retry = Retry.of("get Ingestion Auth Token resources", retryConfig);
                    CheckedFunction0<KustoOperationResult> retryExecute = Retry.decorateCheckedSupplier(retry,
                            () -> client.execute(Commands.IDENTITY_GET_COMMAND));
                    KustoOperationResult identityTokenResult = retryExecute.apply();
                    if (identityTokenResult != null
                            && identityTokenResult.hasNext()
                            && !identityTokenResult.getResultTables().isEmpty()) {
                        KustoResultSetTable resultTable = identityTokenResult.next();
                        resultTable.next();
                        identityToken = resultTable.getString(0);
                    }
                    refreshedAtLeastOnce = true;
                    ingestionAuthTokenRefreshedOnce.signalAll();
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

    private void init() {
        scheduleRefreshIngestionResourcesTask(0L);
        scheduleRefreshIngestionAuthTokenTask(0L);
    }

    // If we combined these 2 methods, ensuring we're using distinct synchronized locks for both would be inelegant
    private synchronized void scheduleRefreshIngestionResourcesTask(Long delay) {
        if (timer != null) {
            if (refreshIngestionResourcesTask != null) {
                refreshIngestionResourcesTask.cancel();
            }

            refreshIngestionResourcesTask = new RefreshIngestionResourcesTask();
            timer.schedule(refreshIngestionResourcesTask, delay, defaultRefreshTime);
        }
    }

    private synchronized void scheduleRefreshIngestionAuthTokenTask(Long delay) {
        if (timer != null) {
            if (refreshIngestionAuthTokenTask != null) {
                refreshIngestionAuthTokenTask.cancel();
            }

            refreshIngestionAuthTokenTask = new RefreshIngestionAuthTokenTask();
            timer.schedule(refreshIngestionAuthTokenTask, delay, defaultRefreshTime);
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
            // Scheduling the task with no delay will force it to try now, but with its normal retry logic
            scheduleRefreshIngestionAuthTokenTask(0L);

            // Wait until the refreshIngestionAuthTokenTask.Run() started
            boolean tryAgain = true;
            while (!refreshIngestionAuthTokenTask.isRefreshedAtLeastOnce() && tryAgain) {
                try {
                    ingestionAuthTokenLock.writeLock().lock();
                    ingestionAuthTokenRefreshedOnce.awaitNanos(REFRESH_WITH_RETRIES_TIMEOUT_NANOS);
                    tryAgain = false; // Either refresh succeeded, or all refreshes timed out (~5x)
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    ingestionAuthTokenLock.writeLock().unlock();
                }
            }

            try {
                ingestionAuthTokenLock.readLock().lock();
                if (identityToken == null) {
                    throw new IngestionServiceException("Unable to get Identity token");
                }
            } finally {
                ingestionAuthTokenLock.readLock().unlock();
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
            // Scheduling the task with no delay will force it to try now, but with its normal retry logic
            scheduleRefreshIngestionResourcesTask(0L);

            // Wait until the refreshIngestionResourcesTask.Run() started
            boolean tryAgain = true;
            while (!refreshIngestionResourcesTask.isRefreshedAtLeastOnce() && tryAgain) {
                try {
                    ingestionResourcesLock.writeLock().lock();
                    ingestionResourcesRefreshedOnce.awaitNanos(REFRESH_WITH_RETRIES_TIMEOUT_NANOS);
                    tryAgain = false; // Either refresh succeeded, or all refreshes timed out (~5x)
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    ingestionResourcesLock.writeLock().unlock();
                }
            }

            // If the write lock is locked, then the read will wait here.
            // In other words if the refresh is running yet, then wait until it ends
            ingestionResourcesLock.readLock().lock();
            try {
                resource = resourceGetter.call();
            } catch (Exception ignore) {
            } finally {
                ingestionResourcesLock.readLock().unlock();
            }
            if (resource == null || resource.empty()) {
                throw new IngestionServiceException("Unable to get ingestion resources for this type: " +
                        (resource == null ? "" : resource.resourceType));
            }
        }

        return resource;
    }

    @Override
    public void reportIngestionResult(ResourceWithSas<?> resource, boolean success) {
        if (storageAccountSet == null) {
            log.warn("StorageAccountSet is null");
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
