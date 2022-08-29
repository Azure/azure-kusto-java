package com.microsoft.azure.kusto.ingest.utils;

import com.azure.core.http.HttpClient;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableClientBuilder;
import reactor.util.annotation.Nullable;

public class TableWithSas {
    private final String uri;
    private final TableClient table;

    public TableWithSas(String url, @Nullable HttpClient httpClient) {
        this.uri = url;
        this.table = TableClientFromUrl(url, httpClient);
    }

    public String getUri() {
        return uri;
    }

    public TableClient getTable() {
        return table;
    }

    public static TableClient TableClientFromUrl(String url, @Nullable HttpClient httpClient) {
        String[] parts = url.split("\\?");
        int tableNameIndex = parts[0].lastIndexOf('/');
        String tableName = parts[0].substring(tableNameIndex + 1);

        return new TableClientBuilder()
                .endpoint(parts[0].substring(0, tableNameIndex))
                .sasToken(parts[1])
                .tableName(tableName)
                .httpClient(httpClient)
                .buildClient();
    }
}
