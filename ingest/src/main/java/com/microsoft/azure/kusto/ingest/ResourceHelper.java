package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.utils.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.utils.ResourceWithSas;

import java.util.List;

public interface ResourceHelper {
    List<ContainerWithSas> getContainers() throws IngestionClientException, IngestionServiceException;

    void reportIngestionResult(ResourceWithSas<?> resource, boolean success);
}
