package com.microsoft.azure.kusto.ingest.utils;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.microsoft.azure.kusto.data.UriUtils;

import java.net.URISyntaxException;

public class ContainerWithSas {
    private final String sas;
    private final BlobContainerClient container;

    public ContainerWithSas(String url, HttpClient httpClient) throws URISyntaxException {
        String[] parts = UriUtils.getSasAndEndpointFromResourceURL(url);
        String endpoint = parts[0];
        String sas = parts[1];
        this.sas = '?' + sas;

        this.container = new BlobContainerClientBuilder()
                .endpoint(endpoint)
                .sasToken(sas)
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
