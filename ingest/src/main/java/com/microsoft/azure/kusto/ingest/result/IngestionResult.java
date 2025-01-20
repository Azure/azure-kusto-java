// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.implementation.models.TableServiceErrorException;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.List;

public interface IngestionResult extends Serializable {

    /**
     * Retrieves the detailed ingestion status of
     * all data ingestion operations into Kusto associated with this com.microsoft.azure.kusto.ingest.IKustoIngestionResult instance.
     */
    Mono<List<IngestionStatus>> getIngestionStatusCollection() throws URISyntaxException, TableServiceErrorException;

    int getIngestionStatusesLength();
}
