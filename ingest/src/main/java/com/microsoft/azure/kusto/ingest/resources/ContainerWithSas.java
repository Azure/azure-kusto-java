package com.microsoft.azure.kusto.ingest.resources;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.microsoft.azure.kusto.data.UriUtils;

import java.net.URISyntaxException;

public class ContainerWithSas implements ResourceWithSas<BlobContainerAsyncClient> {
    private final String sas;
    private final BlobContainerAsyncClient asyncContainer;

    public ContainerWithSas(String url, HttpClient httpClient) throws URISyntaxException {
        String[] parts = UriUtils.getSasAndEndpointFromResourceURL(url);
        String endpoint = parts[0];
        String sas = parts[1];
        this.sas = '?' + sas;

        this.asyncContainer = new BlobContainerClientBuilder()
                .endpoint(endpoint)
                .sasToken(sas)
                .httpClient(httpClient)
                .buildAsyncClient();
    }

    public String getSas() {
        return sas;
    }

    public BlobContainerAsyncClient getAsyncContainer() {
        return asyncContainer;
    }

    @Override
    public String getEndpointWithoutSas() {
        return asyncContainer.getBlobContainerUrl();
    }

    @Override
    public String getAccountName() {
        return asyncContainer.getAccountName();
    }

    @Override
    public BlobContainerAsyncClient getResource() {
        return asyncContainer;
    }
}
