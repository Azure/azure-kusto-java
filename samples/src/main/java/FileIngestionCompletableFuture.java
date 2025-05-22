// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * This class includes a sample of how to use the ingestFromFile() method within a CompletableFuture
 */
public class FileIngestionCompletableFuture {
    public static void main(String[] args) {
        try {
            // Creating the connection string:
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    System.getProperty("clusterPath"),
                    System.getProperty("appId"),
                    System.getProperty("appKey"),
                    System.getProperty("appTenant"));

            CompletableFuture<IngestionResult> cf;
            try (IngestClient client = IngestClientFactory.createClient(csb)) {
                // Creating the ingestion properties:
                IngestionProperties ingestionProperties = new IngestionProperties(
                        System.getProperty("dbName"),
                        System.getProperty("tableName"));
                ingestionProperties.setIngestionMapping(System.getProperty("dataMappingName"), IngestionMapping.IngestionMappingKind.JSON);

                FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"));

                // Ingest From File ASYNC returns a CompletableFuture:
                cf = ingestFromFileAsync(client, fileSourceInfo, ingestionProperties);
            }

            // In case of exception during File Ingestion, a CompletionException will be thrown by the
            // CompletableFuture, that contains in its cause the original exception that occurred during the
            // ingestion itself.
            // In this case we print an error message and the StackTrace of the cause, and return null, Else (if
            // no exception was thrown), the IngestionResult will be returned by the completable future.
            CompletableFuture<IngestionResult> cf2 = cf.exceptionally(ex -> {
                System.err.println("Error in File Ingestion:");
                // Here the getCause() will return the exception from the file ingestion operation
                ex.getCause().printStackTrace();
                return null;
            });

            // The developer can decide how to continue when the CompletableFuture ends. At this case,
            // print a message, and apply the method doSomethingWithIngestionResult() on the IngestionResult:
            cf2.thenRun(() -> System.out.println("File Ingestion ended."));
            cf2.thenAccept(FileIngestionCompletableFuture::doSomethingWithIngestionResult);

            System.out.println("(Press any key to terminate the program)");
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method wraps the client's ingestFromFile() method, and returns a CompletableFuture.
     *
     * @param client              IngestClient that is connected to the Kusto cluster.
     * @param fileSourceInfo      The specific FileSourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return a {@link CompletableFuture}
     */
    private static CompletableFuture<IngestionResult> ingestFromFileAsync(
            IngestClient client, FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return client.ingestFromFile(fileSourceInfo, ingestionProperties);
                    } catch (IngestionClientException | IngestionServiceException e) {
                        throw new CompletionException(e);
                    }
                });
    }

    /**
     * In this example we just printing a message to the standard output, but the user can decide what to do here.
     */
    private static void doSomethingWithIngestionResult(IngestionResult ingestionResult) {
        if (ingestionResult != null) {
            System.out.println("IngestionResults: " + ingestionResult.toString());
        } else {
            System.out.println("No IngestionResults available");
        }

    }
}
