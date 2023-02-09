// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.TableClient;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class IngestionStatusInTableDescription implements Serializable {
    @JsonProperty
    private String tableConnectionString;

    @JsonProperty
    private String partitionKey;

    @JsonProperty
    private String rowKey;

    @JsonIgnore
    private transient TableClient tableClient;

    @JsonIgnore
    private String tableUri;

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setTableConnectionString(String tableConnectionString) {
        this.tableConnectionString = tableConnectionString;
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

    public void setTableUri(String tableUri) {
        this.tableUri = tableUri;
    }

    public String getTableUri(){
        return this.tableUri;
    }

    public void setTableClient(TableClient tableClient) {
        this.tableClient = tableClient;
    }
}
