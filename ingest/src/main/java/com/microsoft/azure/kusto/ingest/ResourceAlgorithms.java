package com.microsoft.azure.kusto.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.instrumentation.FunctionOneException;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.Tracer;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.resources.RankedStorageAccount;
import com.microsoft.azure.kusto.ingest.resources.ResourceWithSas;
import com.microsoft.azure.kusto.ingest.utils.SecurityUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ResourceAlgorithms {
    private static final int RETRY_COUNT = 3;
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ResourceAlgorithms() {
    }

    private static <TInner, TWrapper extends ResourceWithSas<TInner>, TOut> TOut resourceActionWithRetries(ResourceManager resourceManager,
            List<TWrapper> resources, FunctionOneException<TOut, TWrapper, Exception> action, String actionName, Map<String, String> additionalAttributes)
            throws IngestionClientException {

        if (resources.isEmpty()) {
            throw new IngestionClientException(String.format("%s: No resources were provided.", actionName));
        }

        List<Map<String, String>> totalAttributes = new ArrayList<>();

        for (int i = 0; i < RETRY_COUNT; i++) {
            TWrapper resource = resources.get(i % resources.size());
            try {
                Map<String, String> attributes = new HashMap<>();
                attributes.put("resource", resource.getEndpointWithoutSas());
                attributes.put("account", resource.getAccountName());
                attributes.put("type", resource.getClass().getName());
                attributes.put("retry", String.valueOf(i));
                attributes.putAll(additionalAttributes);
                totalAttributes.add(attributes);

                return MonitoredActivity.invoke((FunctionOneException<TOut, Tracer.Span, Exception>) (Tracer.Span span) -> {
                    try {
                        TOut result = action.apply(resource);
                        resourceManager.reportIngestionResult(resource, true);
                        return result;
                    } catch (Exception e) {
                        resourceManager.reportIngestionResult(resource, false);
                        span.addException(e);
                        throw e;
                    }
                }, actionName, attributes);
            } catch (Exception e) {
                log.warn(String.format("Error during retry %d of %d for %s", i + 1, RETRY_COUNT, actionName), e);
            }
        }
        throw new IngestionClientException(String.format("%s: All %d retries failed - used resources: %s", actionName, RETRY_COUNT,
                totalAttributes.stream().map(x -> String.format("%s (%s)", x.get("resource"), x.get("account"))).collect(Collectors.joining(", "))));
    }

    public static void postToQueueWithRetries(ResourceManager resourceManager, AzureStorageClient azureStorageClient, IngestionBlobInfo blob)
            throws IngestionClientException, IngestionServiceException, JsonProcessingException {
        ObjectMapper objectMapper = Utils.getObjectMapper();
        String message = objectMapper.writeValueAsString(blob);
        resourceActionWithRetries(resourceManager, resourceManager.getShuffledQueues(), queue -> {
            azureStorageClient.postMessageToQueue(queue.getQueue(), message);
            return null;
        }, "ResourceAlgorithms.postToQueueWithRetries",
                Collections.singletonMap("blob", SecurityUtils.removeSecretsFromUrl(blob.getBlobPath())));
    }

    public static String uploadStreamToBlobWithRetries(ResourceManager resourceManager, AzureStorageClient azureStorageClient, InputStream stream,
            String blobName, boolean shouldCompress)
            throws IngestionClientException, IngestionServiceException {
        return resourceActionWithRetries(resourceManager, resourceManager.getShuffledContainers(), container -> {
            azureStorageClient.uploadStreamToBlob(stream, blobName, container.getContainer(), shouldCompress);
            return (container.getContainer().getBlobContainerUrl() + "/" + blobName + container.getSas());
        }, "ResourceAlgorithms.uploadLocalFileWithRetries", Collections.emptyMap());
    }

    public static String uploadLocalFileWithRetries(ResourceManager resourceManager, AzureStorageClient azureStorageClient, File file, String blobName,
            boolean shouldCompress)
            throws IngestionClientException, IngestionServiceException {
        return resourceActionWithRetries(resourceManager, resourceManager.getShuffledContainers(), container -> {
            azureStorageClient.uploadLocalFileToBlob(file, blobName, container.getContainer(), shouldCompress);
            return (container.getContainer().getBlobContainerUrl() + "/" + blobName + container.getSas());
        }, "ResourceAlgorithms.uploadLocalFileWithRetries", Collections.emptyMap());
    }

    @NotNull
    public static <T> List<T> roundRobinNestedList(@NotNull List<List<T>> validResources) {
        int longestResourceList = validResources.stream().mapToInt(List::size).max().orElse(0);

        // Go from 0 to the longest list length
        return IntStream.range(0, longestResourceList).boxed()
                // This flat maps combines all the inner lists
                .flatMap(i ->
                // For each list, get the i'th element if it exists, or null otherwise (if the list is shorter)
                validResources.stream().map(r -> r.size() > i ? r.get(i) : null)
                        // Remove nulls
                        .filter(Objects::nonNull))
                // So we combine the list of the first element of each list, then the second element, etc.
                .collect(Collectors.toList());
    }

    public static <T extends ResourceWithSas<?>> List<T> getShuffledResources(List<RankedStorageAccount> shuffledAccounts, List<T> resourceOfType) {
        Map<String, List<T>> accountToResourcesMap = groupResourceByAccountName(resourceOfType);

        List<List<T>> validResources = shuffledAccounts.stream()
                // For each shuffled account, get the resources for that account
                .map(account -> accountToResourcesMap.get(account.getAccountName()))
                // Remove nulls and empty lists
                .filter(resourceList -> resourceList != null && !resourceList.isEmpty())
                .collect(Collectors.toList());

        return roundRobinNestedList(validResources);
    }

    private static <T extends ResourceWithSas<?>> Map<String, List<T>> groupResourceByAccountName(List<T> resourceSet) {
        if (resourceSet == null || resourceSet.isEmpty()) {
            return Collections.emptyMap();
        }
        return resourceSet.stream().collect(Collectors.groupingBy(ResourceWithSas::getAccountName, Collectors.toList()));
    }

}
