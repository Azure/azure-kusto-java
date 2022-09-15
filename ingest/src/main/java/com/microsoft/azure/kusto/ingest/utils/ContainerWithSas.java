package com.microsoft.azure.kusto.ingest.utils;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

public class ContainerWithSas {
    private final String sas;
    private final BlobContainerClient container;

    public ContainerWithSas(String url, HttpClient httpClient) {
        String[] parts = url.split("\\?");
        this.sas = '?' + parts[1];
        this.container = new BlobContainerClientBuilder()
                .endpoint(parts[0])
                .sasToken(parts[1])
                .httpClient(httpClient)
                .buildClient();
    }

    public String getSas() {
        return sas;
    }

    public BlobContainerClient getContainer() {
        return container;
    }

    public String getEndpoint() {
        return container.getBlobContainerUrl() + sas;
    }
}
