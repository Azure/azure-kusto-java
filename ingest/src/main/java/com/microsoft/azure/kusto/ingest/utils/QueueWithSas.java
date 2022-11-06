package com.microsoft.azure.kusto.ingest.utils;

import com.azure.core.http.HttpClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.microsoft.azure.kusto.data.UriUtils;
import reactor.util.annotation.Nullable;

import java.net.URISyntaxException;

public class QueueWithSas {
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
}
