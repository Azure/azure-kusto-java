package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.instrumentation.FunctionOneException;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.Tracer;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.resources.ResourceWithSas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResourceAlgorithms {
    private static final int RETRY_COUNT = 3;
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ResourceAlgorithms() {
    }

    private static <TInner, TWrapper extends ResourceWithSas<TInner>, TOut> TOut resourceActionWithRetries(ResourceManager resourceManager,
            List<TWrapper> resources, FunctionOneException<TOut, TWrapper, Exception> action, String actionName)
            throws IngestionClientException {

        if (resources.isEmpty()) {
            throw new IngestionClientException("No resources found");
        }

        for (int i = 0; i < RETRY_COUNT; i++) {
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
                return MonitoredActivity.invoke((FunctionOneException<TOut, Tracer.Span, Exception>) (Tracer.Span span) -> {
                    try {
                        TOut result = action.apply(resource);
                        resourceManager.reportIngestionResult(resource, true);
                        return result;
                    } catch (Exception e) {
                        span.addException(e);
                        throw e;
                    }
                }, actionName, attributes);
            } catch (Exception e) {
                log.warn(String.format("Error during retry %d of %d for %s", i + 1, RETRY_COUNT, actionName), e);
                resourceManager.reportIngestionResult(resource, false);
            }
        }
        throw new IngestionClientException("Retry count exceeded");
    }

    public static void postToQueueWithRetries(ResourceManager resourceManager, AzureStorageClient azureStorageClient, String message)
            throws IngestionClientException, IngestionServiceException {
        resourceActionWithRetries(resourceManager, resourceManager.getShuffledQueues(), queue -> {
            azureStorageClient.postMessageToQueue(queue.getQueue(), message);
            return null;
        }, "ResourceAlgorithms.postToQueueWithRetries");
    }

    public static String uploadStreamToBlobWithRetries(ResourceManager resourceManager, AzureStorageClient azureStorageClient, InputStream stream,
            String blobName, boolean shouldCompress)
            throws IngestionClientException, IngestionServiceException {
        return resourceActionWithRetries(resourceManager, resourceManager.getShuffledContainers(), container -> {
            azureStorageClient.uploadStreamToBlob(stream, blobName, container.getContainer(), shouldCompress);
            return (container.getContainer().getBlobContainerUrl() + "/" + blobName + container.getSas());
        }, "ResourceAlgorithms.uploadLocalFileWithRetries");
    }

    public static String uploadLocalFileWithRetries(ResourceManager resourceManager, AzureStorageClient azureStorageClient, File file, String blobName,
            boolean shouldCompress)
            throws IngestionClientException, IngestionServiceException {
        return resourceActionWithRetries(resourceManager, resourceManager.getShuffledContainers(), container -> {
            azureStorageClient.uploadLocalFileToBlob(file, blobName, container.getContainer(), shouldCompress);
            return (container.getContainer().getBlobContainerUrl() + "/" + blobName + container.getSas());
        }, "ResourceAlgorithms.uploadLocalFileWithRetries");
    }
}
