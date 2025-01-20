package com.microsoft.azure.kusto.ingest.resources;

import com.azure.core.http.HttpClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.queue.QueueAsyncClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.microsoft.azure.kusto.data.UriUtils;
import reactor.util.annotation.Nullable;

import java.net.URISyntaxException;

public class QueueWithSas implements ResourceWithSas<QueueAsyncClient> {
    private final String sas;
    private final QueueAsyncClient queueAsyncClient;

    public QueueWithSas(String url, HttpClient httpClient, @Nullable RequestRetryOptions retryOptions) throws URISyntaxException {
        String[] parts = UriUtils.getSasAndEndpointFromResourceURL(url);
        this.sas = '?' + parts[1];
        this.queueAsyncClient = new QueueClientBuilder()
                .endpoint(parts[0])
                .sasToken(parts[1])
                .httpClient(httpClient)
                .retryOptions(retryOptions)
                .buildAsyncClient();
    }

    public String getSas() {
        return sas;
    }

    public QueueAsyncClient getAsyncQueue() {
        return queueAsyncClient;
    }

    public String getEndpoint() {
        return queueAsyncClient.getQueueUrl() + sas;
    }

    @Override
    public String getEndpointWithoutSas() {
        return queueAsyncClient.getQueueUrl();
    }

    @Override
    public String getAccountName() {
        return queueAsyncClient.getAccountName();
    }

    @Override
    public QueueAsyncClient getResource() {
        return queueAsyncClient;
    }
}
