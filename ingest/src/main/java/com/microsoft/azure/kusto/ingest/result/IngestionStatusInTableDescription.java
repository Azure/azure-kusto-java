// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.TableClient;

import java.io.Serializable;

public class IngestionStatusInTableDescription implements Serializable {
    private String partitionKey;
    private String rowKey;
    private TableClient tableClient;

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public TableClient getTableClient() {
        return tableClient;
    }

    public void setTableClient(TableClient tableClient) {
        this.tableClient = tableClient;
    }
}
