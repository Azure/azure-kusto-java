package com.microsoft.azure.kusto.ingest;

import com.azure.data.tables.TableClient;

import static com.microsoft.azure.kusto.ingest.AzureStorageClient.TableClientFromUrl;

public class TableWithSas {
    private final String uri;
    private final TableClient table;

    TableWithSas(String url) {
        this.uri = url;
        this.table = TableClientFromUrl(url);
    }

    public String getUri() {
        return uri;
    }

    public TableClient getTable() {
        return table;
    }
}
