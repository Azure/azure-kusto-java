package com.microsoft.azure.kusto.ingest.utils;

import com.azure.core.http.HttpClient;
import com.azure.core.http.policy.RetryOptions;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import reactor.util.annotation.Nullable;

public class QueueWithSas {
    private final String sas;
    private final QueueClient queue;

    public QueueWithSas(String url, HttpClient httpClient, @Nullable RetryOptions retryOptions) {
        String[] parts = url.split("\\?");
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
