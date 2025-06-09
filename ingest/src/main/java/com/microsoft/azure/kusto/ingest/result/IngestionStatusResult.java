// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.implementation.models.TableServiceErrorException;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

public class IngestionStatusResult implements IngestionResult {

    private IngestionStatus ingestionStatus;

    public IngestionStatusResult(IngestionStatus ingestionStatus) {
        this.ingestionStatus = ingestionStatus;
    }

    @Override
    public Mono<List<IngestionStatus>> getIngestionStatusCollectionAsync() {
        if (ingestionStatus != null) {
            return Mono.just(Collections.singletonList(ingestionStatus));
        }

        return Mono.empty();
    }

    @Override
    public List<IngestionStatus> getIngestionStatusCollection() throws URISyntaxException, TableServiceErrorException {
        return getIngestionStatusCollectionAsync().block();
    }
}
