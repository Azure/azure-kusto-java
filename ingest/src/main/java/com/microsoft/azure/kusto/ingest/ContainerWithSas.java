package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

public class ContainerWithSas {
    private final String sas;
    private final BlobContainerClient container;
    ContainerWithSas(String url, HttpClient httpClient){
        this.sas = '?' + url.split("\\?")[1];
        this.container  = new BlobContainerClientBuilder()
                .endpoint(url)
                .httpClient(httpClient)
                .buildClient();
    }

    public String getSas() {
        return sas;
    }

    public BlobContainerClient getContainer() {
        return container;
    }
}
