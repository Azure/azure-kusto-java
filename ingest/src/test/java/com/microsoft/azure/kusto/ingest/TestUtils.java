package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.utils.ContainerWithSas;
import com.microsoft.azure.kusto.ingest.utils.QueueWithSas;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;

import java.net.URISyntaxException;

public class TestUtils {
    static QueueWithSas queueWithSasFromQueueName(String queueName) {
        try {
            return new QueueWithSas(String.format("https://storage.queue.core.windows.net/%s?sas", queueName), null, null);
        } catch (URISyntaxException ex){
            return null;
        }}

    static ContainerWithSas containerWithSasFromContainerName(String containerName) {
       try {
            return new ContainerWithSas(String.format("https://storage.blob.core.windows.net/%s?sas", containerName), null);
        } catch (URISyntaxException ex){
            return null;
        }
    }

    static TableWithSas tableWithSasFromTableName(String tableName) {
      try {
          return new TableWithSas(String.format("https://storage.table.core.windows.net/%s?sas", tableName), null);
      } catch (URISyntaxException ex){
            return null;
      }
}
}
