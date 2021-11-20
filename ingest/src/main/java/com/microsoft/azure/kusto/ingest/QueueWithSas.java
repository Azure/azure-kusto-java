package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;

public class QueueWithSas {
    private final String sas;
    private final QueueClient queue;

    QueueWithSas(String url) {
        String[] parts = url.split("\\?");
        this.sas = '?' + parts[1];
        this.queue = new QueueClientBuilder()
                .endpoint(parts[0])
                .sasToken(parts[1])
                .buildClient();
    }

    public String getSas() {
        return sas;
    }

    public QueueClient getQueue() {
        return queue;
    }
}
