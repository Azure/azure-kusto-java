package com.microsoft.azure.kusto.ingest.resources;

import com.azure.core.http.HttpClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.implementation.AzureQueueStorageImpl;
import com.microsoft.azure.kusto.data.UriUtils;
import reactor.util.annotation.Nullable;

import java.net.URISyntaxException;

public class QueueWithSas implements ResourceWithSas<QueueClient> {
    private final String sas;
    private final QueueClient queue;

    public QueueWithSas(String url, HttpClient httpClient, @Nullable RequestRetryOptions retryOptions) throws URISyntaxException {
        String[] parts = UriUtils.getSasAndEndpointFromResourceURL(url);
        this.sas = '?' + parts[1];
        this.queue = new QueueClientBuilder()
                .endpoint(parts[0])
                .sasToken(parts[1])
                .httpClient(httpClient)
                .retryOptions(retryOptions)
                .buildClient();
    }

    public String getSas() {
        return sas;
    }

    public QueueClient getQueue() {
        return queue;
    }

    public String getEndpoint() {
        return queue.getQueueUrl() + sas;
    }

    @Override
    public String getEndpointWithoutSas() {
        return queue.getQueueUrl();
    }

    @Override
    public String getAccountName() {
        return queue.getAccountName();
    }

    @Override
    public QueueClient getResource() {
        return queue;
    }
}
