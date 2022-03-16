// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableOperation;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

public class TableReportIngestionResult implements IngestionResult {
    private final List<IngestionStatusInTableDescription> descriptors;

    public TableReportIngestionResult(List<IngestionStatusInTableDescription> descriptors) {
        this.descriptors = descriptors;
    }

    @Override
    public List<IngestionStatus> getIngestionStatusCollection() throws StorageException, URISyntaxException {
        List<IngestionStatus> results = new LinkedList<>();
        for (IngestionStatusInTableDescription descriptor : descriptors) {
            CloudTable table = new CloudTable(new URI(descriptor.getTableConnectionString()));
            TableOperation operation = TableOperation
                    .retrieve(
                            descriptor.getPartitionKey(),
                            descriptor.getRowKey(),
                            IngestionStatus.class);
            results.add(table.execute(operation).getResultAsType());
        }

        return results;
    }

    @Override
    public int getIngestionStatusesLength() {
        return descriptors.size();
    }
}
