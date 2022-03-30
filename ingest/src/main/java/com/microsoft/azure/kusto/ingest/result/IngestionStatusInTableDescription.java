// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import java.io.Serializable;

public class IngestionStatusInTableDescription implements Serializable {
    private String tableConnectionString;
    private String partitionKey;
    private String rowKey;

    public String getTableConnectionString() {
        return tableConnectionString;
    }

    public void setTableConnectionString(String tableConnectionString) {
        this.tableConnectionString = tableConnectionString;
    }

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
}