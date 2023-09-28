// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.TableClient;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;

public class IngestionStatusInTableDescription implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    @JsonProperty
    private String tableConnectionString;

    @JsonProperty
    private String partitionKey;

    @JsonProperty
    private String rowKey;

    @JsonIgnore
    private transient TableClient tableClient;

    public String getTableConnectionString() {
        return this.tableConnectionString;
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

    public TableClient getTableClient() {
        if (tableClient == null) {
            try {
                tableClient = TableWithSas.tableClientFromUrl(getTableConnectionString(), null);
            } catch (URISyntaxException uriSyntaxException) {
                log.error("TableConnectionString could not be parsed as URI reference.", uriSyntaxException);
                return null;
            }
        }

        return tableClient;
    }

    public void setTableClient(TableClient tableClient) {
        this.tableClient = tableClient;
    }
}
