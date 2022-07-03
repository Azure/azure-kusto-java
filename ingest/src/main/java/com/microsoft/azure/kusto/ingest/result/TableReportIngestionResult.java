// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.microsoft.azure.kusto.data.HttpClientProperties;
import com.microsoft.azure.kusto.ingest.IngestionUtils;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableOperation;

import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

public class TableReportIngestionResult implements IngestionResult {
    private final List<IngestionStatusInTableDescription> descriptors;
    private final OperationContext operationContext;

    public TableReportIngestionResult(List<IngestionStatusInTableDescription> descriptors, @Nullable HttpClientProperties properties) {
        this.descriptors = descriptors;
        this.operationContext = IngestionUtils.httpClientPropertiesToOperationContext(properties);
    }

    @Override
    public List<IngestionStatus> getIngestionStatusCollection() throws StorageException, URISyntaxException {
        List<IngestionStatus> results = new LinkedList<>();
        for (IngestionStatusInTableDescription descriptor : descriptors) {
            CloudTable table = new CloudTable(new URI(descriptor.getTableConnectionString()));
            TableOperation operation = TableOperation.retrieve(descriptor.getPartitionKey(), descriptor.getRowKey(),
                    IngestionStatus.class);
            results.add(table.execute(operation, null, operationContext).getResultAsType());
        }

        return results;
    }

    @Override
    public int getIngestionStatusesLength() {
        return descriptors.size();
    }
}
