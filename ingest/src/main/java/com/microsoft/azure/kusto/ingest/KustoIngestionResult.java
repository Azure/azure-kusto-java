package com.microsoft.azure.kusto.ingest;

import java.net.URISyntaxException;
import java.util.List;

import com.microsoft.azure.storage.StorageException;

public interface KustoIngestionResult {
    /// <summary>
    /// Retrieves the detailed ingestion status of 
    /// all data ingestion operations into Kusto associated with this com.microsoft.azure.kusto.ingest.IKustoIngestionResult instance.
    /// </summary>
    List<IngestionStatus> GetIngestionStatusCollection() throws StorageException, URISyntaxException;
}
