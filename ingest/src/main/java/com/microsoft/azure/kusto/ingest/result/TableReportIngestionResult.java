// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.TableAsyncClient;
import com.azure.data.tables.implementation.models.TableServiceErrorException;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class TableReportIngestionResult implements IngestionResult {
    private final List<IngestionStatusInTableDescription> descriptors;

    public TableReportIngestionResult(List<IngestionStatusInTableDescription> descriptors) {
        this.descriptors = descriptors;
    }

    @Override
    public Mono<List<IngestionStatus>> getIngestionStatusCollectionAsync() throws TableServiceErrorException {
        List<Mono<IngestionStatus>> ingestionStatusMonos = descriptors.stream()
                .map(descriptor -> {
                    TableAsyncClient tableAsyncClient = descriptor.getTableAsyncClient();
                    if (tableAsyncClient != null) {
                        return tableAsyncClient.getEntity(descriptor.getPartitionKey(), descriptor.getRowKey())
                                .map(IngestionStatus::fromEntity);
                    } else {
                        return Mono.<IngestionStatus>empty();
                    }
                })
                .collect(Collectors.toList());

        return Mono.zip(ingestionStatusMonos, results -> {
            List<IngestionStatus> resultList = new LinkedList<>();
            for (Object result : results) {
                resultList.add((IngestionStatus) result);
            }
            return resultList;
        });
    }

    @Override
    public List<IngestionStatus> getIngestionStatusCollection() throws URISyntaxException, TableServiceErrorException {
        return getIngestionStatusCollectionAsync().block();
    }
}
