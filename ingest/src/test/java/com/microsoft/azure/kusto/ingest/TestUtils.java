package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.utils.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.utils.QueueWithSas;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;

import java.net.URISyntaxException;

public class TestUtils {
    static QueueWithSas queueWithSasFromQueueName(String queueName) {
        try {
            return new QueueWithSas(String.format("https://storage.queue.core.windows.net/%s?sas", queueName), null, null);
        } catch (URISyntaxException ex) {
            return null;
        }
    }

    static QueueWithSas queueWithSasFromAccountNameAndQueueName(String accountName, String queueName) {
        try {
            return new QueueWithSas(String.format("https://%s.blob.core.windows.net/%s?sas", accountName, queueName), null, null);
        } catch (URISyntaxException ex) {
            return null;
        }
    }

    static ContainerWithSas containerWithSasFromContainerName(String containerName) {
        try {
            return new ContainerWithSas(String.format("https://storage.blob.core.windows.net/%s?sas", containerName), null);
        } catch (URISyntaxException ex) {
            return null;
        }
    }

    static ContainerWithSas containerWithSasFromAccountNameAndContainerName(String accountName, String containerName) {
        try {
            return new ContainerWithSas(String.format("https://%s.blob.core.windows.net/%s?sas", accountName, containerName), null);
        } catch (URISyntaxException ex) {
            return null;
        }
    }

    static String blobWithSasFromAccountNameAndContainerName(String accountName, String containerName, String blobName) {
        return String.format("https://%s.blob.core.windows.net/%s/%s?sas", accountName, containerName, blobName);
    }

    static TableWithSas tableWithSasFromTableName(String tableName) {
        try {
            return new TableWithSas(String.format("https://storage.table.core.windows.net/%s?sas", tableName), null);
        } catch (URISyntaxException ex) {
            return null;
        }
    }
}
