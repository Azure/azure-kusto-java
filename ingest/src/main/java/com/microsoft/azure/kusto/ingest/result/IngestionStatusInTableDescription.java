// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.TableClient;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;

import java.io.Serializable;
import java.net.URISyntaxException;

public class IngestionStatusInTableDescription implements Serializable {
    @JsonProperty
    private String tableConnectionString;

    @JsonProperty
    private String partitionKey;

    @JsonProperty
    private String rowKey;

    @JsonIgnore
    private transient TableClient tableClient;

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setTableConnectionString(String tableConnectionString) {
        this.tableConnectionString = tableConnectionString;
    }

    public String getTableConnectionString() {
        return this.tableConnectionString;
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

    public TableClient getTableClient() throws URISyntaxException {
        if (tableClient == null) {
            tableClient = TableWithSas.TableClientFromUrl(getTableConnectionString(), null);
        }
        return tableClient;
    }

    public void setTableClient(TableClient tableClient) {
        this.tableClient = tableClient;
    }
}
