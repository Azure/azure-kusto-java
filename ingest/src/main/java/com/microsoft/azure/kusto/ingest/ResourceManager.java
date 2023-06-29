// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.HttpClientWrapper;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.ThrottleException;
import com.microsoft.azure.kusto.data.instrumentation.FunctionOneException;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.SupplierTwoExceptions;
import com.microsoft.azure.kusto.data.instrumentation.Tracer;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.resources.RankedStorageAccount;
import com.microsoft.azure.kusto.ingest.resources.RankedStorageAccountSet;
import com.microsoft.azure.kusto.ingest.utils.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.utils.QueueWithSas;
import com.microsoft.azure.kusto.ingest.utils.ResourceWithSas;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.vavr.CheckedFunction0;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ResourceManager implements Closeable, ResourceHelper {
    public static final String SERVICE_TYPE_COLUMN_NAME = "ServiceType";
    private static final long REFRESH_INGESTION_RESOURCES_PERIOD = 1000L * 60 * 60; // 1 hour
    private static final long REFRESH_INGESTION_RESOURCES_PERIOD_ON_FAILURE = 1000L * 60 * 15; // 15 minutes
    private static final int MAX_RETRY_ATTEMPTS = 4;
    private static final long MAX_RETRY_INTERVAL = 1000L * 30;
    private static final long BASE_INTERVAL = 1000L * 2;
    public static int UPLOAD_TIMEOUT_MINUTES = 10;
    private final Client client;
    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Timer timer;
    private final ReadWriteLock ingestionResourcesLock = new ReentrantReadWriteLock();
    private final ReadWriteLock authTokenLock = new ReentrantReadWriteLock();
    private final Long defaultRefreshTime;
    private final Long refreshTimeOnFailure;
    private final HttpClient httpClient;
    private final RetryConfig retryConfig;
    private RequestRetryOptions queueRequestOptions = null;
    private RankedStorageAccountSet storageAccountSet;
    private String identityToken;
    private IngestionResource<ContainerWithSas> containers;
    private IngestionResource<TableWithSas> statusTable;
    private IngestionResource<QueueWithSas> queues;
    private IngestionResource<QueueWithSas> successfulIngestionsQueues;
    private IngestionResource<QueueWithSas> failedIngestionsQueues;

    ResourceManager(Client client, long defaultRefreshTime, long refreshTimeOnFailure, @Nullable CloseableHttpClient httpClient) {
        this.defaultRefreshTime = defaultRefreshTime;
        this.refreshTimeOnFailure = refreshTimeOnFailure;
        this.client = client;
        timer = new Timer(true);
        // Using ctor with client so that the dependency is used
        this.httpClient = httpClient == null
                ? new NettyAsyncHttpClientBuilder().responseTimeout(Duration.ofMinutes(UPLOAD_TIMEOUT_MINUTES)).build()
                : new HttpClientWrapper(httpClient);
        retryConfig = buildRetryConfig();
        storageAccountSet = new RankedStorageAccountSet();
        init();
    }

    ResourceManager(Client client, @Nullable CloseableHttpClient httpClient) {
        this(client, REFRESH_INGESTION_RESOURCES_PERIOD, REFRESH_INGESTION_RESOURCES_PERIOD_ON_FAILURE, httpClient);
    }

    @Override
    public void close() {
        timer.cancel();
        timer.purge();
    }

    private RetryConfig buildRetryConfig() {
        IntervalFunction sleepConfig = IntervalFunction.ofExponentialRandomBackoff(BASE_INTERVAL,
                IntervalFunction.DEFAULT_MULTIPLIER,
                IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR,
                MAX_RETRY_INTERVAL);
        return RetryConfig.custom()
                .maxAttempts(MAX_RETRY_ATTEMPTS)
                .intervalFunction(sleepConfig)
                .retryExceptions(ThrottleException.class)
                .build();
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
                    log.error("Error in refreshIngestionAuthToken. " + e.getMessage(), e);
                    timer.schedule(new RefreshIngestionAuthTokenTask(), refreshTimeOnFailure);
                }
            }
        }

        timer.schedule(new RefreshIngestionAuthTokenTask(), 0);
        timer.schedule(new RefreshIngestionResourcesTask(), 0);
    }

    @Override
    public List<ContainerWithSas> getContainers() throws IngestionClientException, IngestionServiceException {
        IngestionResource<ContainerWithSas> containers = getResourceSet(() -> this.containers);
        return storageAccountSet.getShuffledResources(
                groupResourceByAccountName(containers.getResourcesList()));
    }

    private <T extends ResourceWithSas<?>> Map<String, List<T>> groupResourceByAccountName(List<T> resourceSet) {
        if (resourceSet == null || resourceSet.isEmpty()) {
            return Collections.emptyMap();
        }
        return resourceSet.stream().collect(Collectors.groupingBy(ResourceWithSas::getAccountName, Collectors.toList()));
    }

    public List<QueueWithSas> getQueues() throws IngestionClientException, IngestionServiceException {
        IngestionResource<QueueWithSas> queues = getResourceSet(() -> this.queues);
        return storageAccountSet.getShuffledResources(
                groupResourceByAccountName(queues.getResourcesList()));
    }

    public TableWithSas getStatusTable() throws IngestionClientException, IngestionServiceException {
        return getResource(() -> this.statusTable);
    }

    public QueueWithSas getFailedQueue() throws IngestionClientException, IngestionServiceException {
        return getResource(() -> this.failedIngestionsQueues);
    }

    public QueueWithSas getSuccessfulQueue() throws IngestionClientException, IngestionServiceException {
        return getResource(() -> this.successfulIngestionsQueues);
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

    private void addIngestionResource(String resourceTypeName, String storageUrl) throws URISyntaxException {
        ResourceType resourceType = ResourceType.findByResourceTypeName(resourceTypeName);
        switch (resourceType) {
            case TEMP_STORAGE:
                this.containers.addResource(new ContainerWithSas(storageUrl, httpClient));
                break;
            case INGESTIONS_STATUS_TABLE:
                this.statusTable.addResource(new TableWithSas(storageUrl, httpClient));
                break;
            case SECURED_READY_FOR_AGGREGATION_QUEUE:
                this.queues.addResource(new QueueWithSas(storageUrl, httpClient, this.queueRequestOptions));
                break;
            case SUCCESSFUL_INGESTIONS_QUEUE:
                this.successfulIngestionsQueues.addResource(new QueueWithSas(storageUrl, httpClient, this.queueRequestOptions));
                break;
            case FAILED_INGESTIONS_QUEUE:
                this.failedIngestionsQueues.addResource(new QueueWithSas(storageUrl, httpClient, this.queueRequestOptions));
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
                this.containers = new IngestionResource<>(ResourceType.TEMP_STORAGE);
                this.queues = new IngestionResource<>(ResourceType.SECURED_READY_FOR_AGGREGATION_QUEUE);
                this.successfulIngestionsQueues = new IngestionResource<>(ResourceType.SUCCESSFUL_INGESTIONS_QUEUE);
                this.failedIngestionsQueues = new IngestionResource<>(ResourceType.FAILED_INGESTIONS_QUEUE);
                this.statusTable = new IngestionResource<>(ResourceType.INGESTIONS_STATUS_TABLE);
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
                        addIngestionResource(resourceTypeName, storageUrl);
                    }
                }
                populateStorageAccounts();
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

    private void populateStorageAccounts() {
        RankedStorageAccountSet tempAccount = new RankedStorageAccountSet();

        if (this.queues != null) {
            for (QueueWithSas queue : this.queues.getResourcesList()) {
                RankedStorageAccount previousAccount = this.storageAccountSet.getAccount(queue.getQueue().getAccountName());
                if (previousAccount != null) {
                    tempAccount.addAccount(previousAccount);
                } else {
                    tempAccount.addAccount(queue.getQueue().getAccountName());
                }
            }
        }
        if (this.containers != null) {
            for (ContainerWithSas queue : this.containers.getResourcesList()) {
                RankedStorageAccount previousAccount = this.storageAccountSet.getAccount(queue.getContainer().getAccountName());
                if (previousAccount != null) {
                    tempAccount.addAccount(previousAccount);
                } else {
                    tempAccount.addAccount(queue.getContainer().getAccountName());
                }
            }
        }

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

    private int calculateRetries(int size) {
        return size < 3 ? size : size / 2 + 1; // Calculation is based on dotnet
    }

    private <TInner, TWrapper extends ResourceWithSas<TInner>, TOut> TOut resourceActionWithRetries(List<TWrapper> resources, FunctionOneException<TOut, TWrapper, Exception> action, String actionName)
            throws IngestionClientException {

        if (resources.isEmpty()) {
            throw new IngestionClientException("No resources found");
        }
        int retries = calculateRetries(resources.size());

        for (int i = 0; i < retries; i++) {
            if (i >= resources.size()) {
                throw new IngestionClientException("Retry count exceeded");
            }
            TWrapper resource = resources.get(i);
            try {

                Map<String, String> attributes = new HashMap<>();
                attributes.put("resource", resource.getEndpointWithoutSas());
                attributes.put("account", resource.getAccountName());
                attributes.put("type", resource.getClass().getName());
                attributes.put("retry", String.valueOf(i));
                MonitoredActivity.invoke((FunctionOneException<TOut, Tracer.Span, Exception>) (Tracer.Span span) -> {
                    try {
                        TOut result = action.apply(resource);
                        reportIngestionResult(resource, true);
                        return result;
                    } catch (Exception e) {
                        span.addException(e);
                        throw e;
                    }
                }, actionName, attributes);
            } catch (Exception e) {
                log.warn(String.format("Error during retry %d of %d for %s", i + 1, retries, actionName), e);
                reportIngestionResult(resource, false);
            }
        }
        throw new IngestionClientException("Retry count exceeded");
    }

    void postToQueueWithRetries(AzureStorageClient azureStorageClient, String message) throws IngestionClientException, IngestionServiceException {
        resourceActionWithRetries(getQueues(), queue -> {
            azureStorageClient.postMessageToQueue(queue.getQueue(), message);
            return null;
        }, "ResourceManager.postToQueueWithRetries");
    }

    String uploadStreamToBlobWithRetries(AzureStorageClient azureStorageClient, InputStream stream, String blobName, boolean shouldCompress) throws IngestionClientException, IngestionServiceException {
        return resourceActionWithRetries(getContainers(), container -> {
            azureStorageClient.uploadStreamToBlob(stream, blobName, container.getContainer(), shouldCompress);
            return (container.getContainer().getBlobContainerUrl() + "/" + blobName + container.getSas());
        }, "ResourceManager.uploadLocalFileWithRetries");
    }

    String uploadLocalFileWithRetries(AzureStorageClient azureStorageClient, File file, String blobName, boolean shouldCompress) throws IngestionClientException, IngestionServiceException {
        return resourceActionWithRetries(getContainers(), container -> {
            azureStorageClient.uploadLocalFileToBlob(file, blobName, container.getContainer(), shouldCompress);
            return (container.getContainer().getBlobContainerUrl() + "/" + blobName + container.getSas());
        }, "ResourceManager.uploadLocalFileWithRetries");
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
            return this.resourcesList.size() == 0;
        }
    }
}
