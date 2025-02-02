// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;

public class IngestionStatusResult implements IngestionResult {

    private IngestionStatus ingestionStatus;

    public IngestionStatusResult(IngestionStatus ingestionStatus) {
        this.ingestionStatus = ingestionStatus;
    }

    @Override
    public Mono<List<IngestionStatus>> getIngestionStatusCollection() {
        return Mono.defer(() -> {
            if (ingestionStatus != null) {
                return Mono.just(Collections.singletonList(ingestionStatus));
            }
            return Mono.empty();
        });
    }

    @Override
    public int getIngestionStatusesLength() {
        return 1;
    }
}
