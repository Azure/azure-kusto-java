// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.auth.HttpClientWrapper;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.ThrottleException;
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
import org.apache.http.impl.client.CloseableHttpClient;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

class ResourceManager implements Closeable, IngestionResourceManager {
    public static final String SERVICE_TYPE_COLUMN_NAME = "ServiceType";
    private static final long REFRESH_INGESTION_RESOURCES_PERIOD = TimeUnit.HOURS.toMillis(1);
    private static final long REFRESH_INGESTION_RESOURCES_PERIOD_ON_FAILURE = TimeUnit.MINUTES.toMillis(15);
    public static final int UPLOAD_TIMEOUT_MINUTES = 10;
    private final Client client;
    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private Timer timer;
    private final ReadWriteLock ingestionResourcesLock = new ReentrantReadWriteLock();
    private final ReadWriteLock authTokenLock = new ReentrantReadWriteLock();
    private final Long defaultRefreshTime;
    private final Long refreshTimeOnFailure;
    private final HttpClient httpClient;
    private final RetryConfig retryConfig;
    private RequestRetryOptions queueRequestOptions = null;
    private RankedStorageAccountSet storageAccountSet;
    private String identityToken;
    private IngestionResourceSet ingestionResourceSet;

    public ResourceManager(Client client, long defaultRefreshTime, long refreshTimeOnFailure, @Nullable CloseableHttpClient httpClient) {
        this.defaultRefreshTime = defaultRefreshTime;
        this.refreshTimeOnFailure = refreshTimeOnFailure;
        this.client = client;
        timer = new Timer(true);
        // Using ctor with client so that the dependency is used
        this.httpClient = httpClient == null
                ? new NettyAsyncHttpClientBuilder().responseTimeout(Duration.ofMinutes(UPLOAD_TIMEOUT_MINUTES)).build()
                : new HttpClientWrapper(httpClient);
        retryConfig = Utils.buildRetryConfig(ThrottleException.class);
        storageAccountSet = new RankedStorageAccountSet();
        init();
    }

    public ResourceManager(Client client, @Nullable CloseableHttpClient httpClient) {
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

    private void init() {
        class RefreshIngestionResourcesTask extends TimerTask {
            @Override
            public void run() {
                try {
                    refreshIngestionResources();
                    timer.schedule(new RefreshIngestionResourcesTask(), defaultRefreshTime);
                } catch (Exception e) {
                    log.error("Error in refreshIngestionResources. " + e.getMessage(), e);
                    if (timer != null) {
                        timer.schedule(new RefreshIngestionResourcesTask(), refreshTimeOnFailure);
                    }
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
                    log.error("Error in refreshIngestionAuthToken. " + e.getMessage(), e);
                    if (timer != null) {
                        timer.schedule(new RefreshIngestionAuthTokenTask(), refreshTimeOnFailure);
                    }
                }
            }
        }

        timer.schedule(new RefreshIngestionAuthTokenTask(), 0);
        timer.schedule(new RefreshIngestionResourcesTask(), 0);
    }

    @Override
    public List<ContainerWithSas> getShuffledContainers() throws IngestionClientException, IngestionServiceException {
        IngestionResource<ContainerWithSas> containers = getResourceSet(() -> this.ingestionResourceSet.containers);
        return ResourceAlgorithms.getShuffledResources(storageAccountSet.getRankedShuffledAccounts(), containers.getResourcesList());
    }

    public List<QueueWithSas> getShuffledQueues() throws IngestionClientException, IngestionServiceException {
        IngestionResource<QueueWithSas> queues = getResourceSet(() -> this.ingestionResourceSet.queues);
        return ResourceAlgorithms.getShuffledResources(storageAccountSet.getRankedShuffledAccounts(), queues.getResourcesList());
    }

    public TableWithSas getStatusTable() throws IngestionClientException, IngestionServiceException {
        return getResource(() -> this.ingestionResourceSet.statusTable);
    }

    public QueueWithSas getFailedQueue() throws IngestionClientException, IngestionServiceException {
        return getResource(() -> this.ingestionResourceSet.failedIngestionsQueues);
    }

    public QueueWithSas getSuccessfulQueue() throws IngestionClientException, IngestionServiceException {
        return getResource(() -> this.ingestionResourceSet.successfulIngestionsQueues);
    }

    public String getIdentityToken() throws IngestionServiceException, IngestionClientException {
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

    public void setQueueRequestOptions(RequestRetryOptions queueRequestOptions) {
        this.queueRequestOptions = queueRequestOptions;
    }

    private <T> T getResource(Callable<IngestionResource<T>> resourceGetter) throws IngestionClientException, IngestionServiceException {
        return getResourceSet(resourceGetter).nextResource();
    }

    private <T> IngestionResource<T> getResourceSet(Callable<IngestionResource<T>> resourceGetter) throws IngestionClientException, IngestionServiceException {
        IngestionResource<T> resource = null;
        try {
            resource = resourceGetter.call();
        } catch (Exception ignore) {
        }

        if (resource == null || resource.empty()) {
            refreshIngestionResources();

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
                ingestionResourceSet.queues.addResource(new QueueWithSas(storageUrl, httpClient, this.queueRequestOptions));
                break;
            case SUCCESSFUL_INGESTIONS_QUEUE:
                ingestionResourceSet.successfulIngestionsQueues.addResource(new QueueWithSas(storageUrl, httpClient, this.queueRequestOptions));
                break;
            case FAILED_INGESTIONS_QUEUE:
                ingestionResourceSet.failedIngestionsQueues.addResource(new QueueWithSas(storageUrl, httpClient, this.queueRequestOptions));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + resourceType);
        }
    }

    private void refreshIngestionResources() throws IngestionServiceException, IngestionClientException {
        // trace refreshIngestionResources
        MonitoredActivity.invoke((SupplierTwoExceptions<Void, IngestionClientException, IngestionServiceException>) () -> {
            refreshIngestionResourcesImpl();
            return null;
        }, "ResourceManager.refreshIngestionResource");
    }

    private void refreshIngestionResourcesImpl() throws IngestionClientException, IngestionServiceException {
        // Here we use tryLock(): If there is another instance doing the refresh, then just skip it.
        if (ingestionResourcesLock.writeLock().tryLock()) {
            try {
                log.info("Refreshing Ingestion Resources");
                IngestionResourceSet ingestionResourceSet = new IngestionResourceSet();
                Retry retry = Retry.of("get ingestion resources", this.retryConfig);
                CheckedFunction0<KustoOperationResult> retryExecute = Retry.decorateCheckedSupplier(retry,
                        () -> client.execute(Commands.INGESTION_RESOURCES_SHOW_COMMAND));
                KustoOperationResult ingestionResourcesResults = retryExecute.apply();
                if (ingestionResourcesResults != null) {
                    KustoResultSetTable table = ingestionResourcesResults.getPrimaryResults();
                    // Add the received values to the new ingestion resources
                    while (table.next()) {
                        String resourceTypeName = table.getString(0);
                        String storageUrl = table.getString(1);
                        addIngestionResource(ingestionResourceSet, resourceTypeName, storageUrl);
                    }
                }
                populateStorageAccounts(ingestionResourceSet);
                this.ingestionResourceSet = ingestionResourceSet;
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

            RankedStorageAccount previousAccount = this.storageAccountSet.getAccount(accountName);
            if (previousAccount != null) {
                tempAccount.addAccount(previousAccount);
            } else {
                tempAccount.addAccount(accountName);
            }
        });

        this.storageAccountSet = tempAccount;
    }

    private void refreshIngestionAuthToken() throws IngestionServiceException, IngestionClientException {
        // trace refreshIngestionAuthToken
        MonitoredActivity.invoke((SupplierTwoExceptions<Void, IngestionClientException, IngestionServiceException>) () -> {
            refreshIngestionAuthTokenImpl();
            return null;
        }, "ResourceManager.refreshIngestionAuthToken");
    }

    private void refreshIngestionAuthTokenImpl() throws IngestionClientException, IngestionServiceException {
        if (authTokenLock.writeLock().tryLock()) {
            try {
                log.info("Refreshing Ingestion Auth Token");
                Retry retry = Retry.of("get Ingestion Auth Token resources", this.retryConfig);
                CheckedFunction0<KustoOperationResult> retryExecute = Retry.decorateCheckedSupplier(retry, () -> client.execute(Commands.IDENTITY_GET_COMMAND));
                KustoOperationResult identityTokenResult = retryExecute.apply();
                if (identityTokenResult != null
                        && identityTokenResult.hasNext()
                        && !identityTokenResult.getResultTables().isEmpty()) {
                    KustoResultSetTable resultTable = identityTokenResult.next();
                    resultTable.next();
                    identityToken = resultTable.getString(0);
                }
            } catch (DataServiceException e) {
                throw new IngestionServiceException(e.getIngestionSource(), "Error refreshing IngestionAuthToken. " + e.getMessage(), e);
            } catch (DataClientException e) {
                throw new IngestionClientException(e.getIngestionSource(), "Error refreshing IngestionAuthToken. " + e.getMessage(), e);
            } catch (Throwable e) {
                throw new IngestionClientException(e.getMessage(), e);
            } finally {
                authTokenLock.writeLock().unlock();
            }
        }
    }

    protected String retrieveServiceType() {
        log.info("Getting version to determine endpoint's ServiceType");
        try {
            KustoOperationResult versionResult = client.execute(Commands.VERSION_SHOW_COMMAND);
            if (versionResult != null && versionResult.hasNext() && !versionResult.getResultTables().isEmpty()) {
                KustoResultSetTable resultTable = versionResult.next();
                resultTable.next();
                return resultTable.getString(SERVICE_TYPE_COLUMN_NAME);
            }
        } catch (DataServiceException e) {
            log.warn("Couldn't retrieve ServiceType because of a service exception executing '.show version'");
            return null;
        } catch (DataClientException e) {
            log.warn("Couldn't retrieve ServiceType because of a client exception executing '.show version'");
            return null;
        }
        log.warn("Couldn't retrieve ServiceType because '.show version' didn't return any records");
        return null;
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
        IngestionResource<ContainerWithSas> containers = new IngestionResource<>(ResourceType.TEMP_STORAGE);;
        IngestionResource<TableWithSas> statusTable = new IngestionResource<>(ResourceType.INGESTIONS_STATUS_TABLE);;
        IngestionResource<QueueWithSas> queues = new IngestionResource<>(ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE);;
        IngestionResource<QueueWithSas> successfulIngestionsQueues = new IngestionResource<>(ResourceType.SUCCESSFUL_INGESTIONS_QUEUE);;
        IngestionResource<QueueWithSas> failedIngestionsQueues = new IngestionResource<>(ResourceType.FAILED_INGESTIONS_QUEUE);;
    }
}
