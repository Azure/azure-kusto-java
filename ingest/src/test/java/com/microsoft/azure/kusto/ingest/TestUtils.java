package com.microsoft.azure.kusto.ingest;

public class TestUtils {
    static QueueWithSas queueWithSasFromQueueName(String queueName) {
        return new QueueWithSas(String.format("https://storage.queue.core.windows.net/%s?sas\"", queueName));
    }

    static ContainerWithSas containerWithSasFromBlobName(String blobName) {
        return new ContainerWithSas(String.format("https://storage.blob.core.windows.net/%s?sas\"", blobName), null);
    }

    static TableWithSas tableWithSasFromTableName(String tableName) {
        return new TableWithSas(String.format("https://storage.table.core.windows.net/%s?sas\"", tableName));
    }
}
