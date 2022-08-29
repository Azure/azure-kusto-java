// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;

public interface IngestionResult extends Serializable {
    /// <summary>
    /// Retrieves the detailed ingestion status of
    /// all data ingestion operations into Kusto associated with this com.microsoft.azure.kusto.ingest.IKustoIngestionResult instance.
    /// </summary>
    List<IngestionStatus> getIngestionStatusCollection() throws URISyntaxException, ParseException;

    int getIngestionStatusesLength();
}
