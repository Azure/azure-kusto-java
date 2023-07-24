package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.resources.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.resources.ResourceWithSas;

import java.util.List;

public interface IngestionResourceManager {

    /**
     * Returns a list of containers with SAS tokens, ranked by their ingestion success rate, and then shuffled.
     * You should use this method for each ingestion operation.
     * @return List of containers with SAS tokens, ranked by their ingestion success rate, and then shuffled.
     */
    List<ContainerWithSas> getShuffledContainers() throws IngestionClientException, IngestionServiceException;

    /**
     * Report the result of an ingestion operation.
     * @param resource The resource that was used to ingest. Can be a container or a queue.
     * @param success Whether the ingestion operation was successful.
     */
    void reportIngestionResult(ResourceWithSas<?> resource, boolean success);
}
