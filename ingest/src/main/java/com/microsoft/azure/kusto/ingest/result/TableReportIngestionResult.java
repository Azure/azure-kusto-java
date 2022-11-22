// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableEntity;

import java.util.LinkedList;
import java.util.List;

public class TableReportIngestionResult implements IngestionResult {
    private final List<IngestionStatusInTableDescription> descriptors;

    public TableReportIngestionResult(List<IngestionStatusInTableDescription> descriptors) {
        this.descriptors = descriptors;
    }

    @Override
    public List<IngestionStatus> getIngestionStatusCollection() {
        List<IngestionStatus> results = new LinkedList<>();
        for (IngestionStatusInTableDescription descriptor : descriptors) {
            TableClient table = descriptor.getTableClient();
            TableEntity entity = table.getEntity(descriptor.getPartitionKey(), descriptor.getRowKey());
            results.add(IngestionStatus.fromEntity(entity));
        }

        return results;
    }

    @Override
    public int getIngestionStatusesLength() {
        return descriptors.size();
    }
}
