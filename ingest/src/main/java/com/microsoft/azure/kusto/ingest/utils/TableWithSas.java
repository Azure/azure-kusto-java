package com.microsoft.azure.kusto.ingest.utils;

import com.azure.core.http.HttpClient;
import com.azure.data.tables.TableAsyncClient;
import com.azure.data.tables.TableClientBuilder;
import com.microsoft.azure.kusto.data.UriUtils;
import reactor.util.annotation.Nullable;

import java.net.URISyntaxException;

public class TableWithSas {
    private final String uri;
    private final TableAsyncClient tableAsyncClient;

    public TableWithSas(String url, @Nullable HttpClient httpClient) throws URISyntaxException {
        this.uri = url;
        this.tableAsyncClient = createTableClientFromUrl(url, httpClient);
    }

    public String getUri() {
        return uri;
    }

    public TableAsyncClient getTableAsyncClient() {
        return tableAsyncClient;
    }

    public static TableAsyncClient createTableClientFromUrl(String url, @Nullable HttpClient httpClient) throws URISyntaxException {
        String[] parts = UriUtils.getSasAndEndpointFromResourceURL(url);
        int tableNameIndex = parts[0].lastIndexOf('/');
        String tableName = parts[0].substring(tableNameIndex + 1);
        return new TableClientBuilder()
                .endpoint(parts[0].substring(0, tableNameIndex))
                .sasToken(parts[1])
                .tableName(tableName)
                .httpClient(httpClient)
                .buildAsyncClient();
    }
}
