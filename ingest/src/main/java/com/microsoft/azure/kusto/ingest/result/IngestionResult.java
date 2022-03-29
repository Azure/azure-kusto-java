// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.microsoft.azure.storage.StorageException;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.List;

public interface IngestionResult extends Serializable {
    /// <summary>
    /// Retrieves the detailed ingestion status of
    /// all data ingestion operations into Kusto associated with this com.microsoft.azure.kusto.ingest.IKustoIngestionResult instance.
    /// </summary>
    List<IngestionStatus> getIngestionStatusCollection() throws StorageException, URISyntaxException;

    int getIngestionStatusesLength();
}
