import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

import java.util.concurrent.CompletableFuture;

public class FileIngestionAsync {
    public static void main(String[] args) {
        try {
            ConnectionStringBuilder csb =
                    ConnectionStringBuilder.createWithAadApplicationCredentials(System.getProperty("clusterPath"),
                            System.getProperty("appId"),
                            System.getProperty("appKey"),
                            System.getProperty("appTenant"));
            IngestClient client = IngestClientFactory.createClient(csb);

            IngestionProperties ingestionProperties = new IngestionProperties(System.getProperty("dbName"),
                    System.getProperty("tableName"));
            ingestionProperties.setJsonMappingName(System.getProperty("dataMappingName"));

            FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"), 0);

            // Ingest From File ASYNC returns a CompletableFuture:
            CompletableFuture<IngestionResult> cf = client.ingestFromFileAsync(fileSourceInfo, ingestionProperties);
            System.out.println("Waiting for ingestion results");

            // In case of exception during File Ingestion, a CompletionException will be thrown by the CompletableFuture,
            // that contains in its cause the original exception that occurred during the ingestion itself.
            // In this case we print an error message and the StackTrace of the cause, and return null, Else (if
            // no exception was thrown), the IngestionResult will be returned by the completable future.
            CompletableFuture<IngestionResult> cf2 = cf.exceptionally(ex -> {
                System.err.println("Error in File Ingestion:");
                // Here the getCause() will return the exception from the file ingestion operation
                ex.getCause().printStackTrace();
                return null;
            });

            // When the CompletableFuture ends and we have an IngestionResult,
            // print a message, and apply the method doSomethingWithIngestionResult() on the result:
            cf2.thenRun(() -> System.out.println("File Ingestion ended."));
            cf2.thenAccept(FileIngestionAsync::doSomethingWithIngestionResult);

            System.out.println("(Press Enter to terminate the program)");
            System.in.read();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void doSomethingWithIngestionResult(IngestionResult ingestionResult){
        if(ingestionResult != null) {
            System.out.println("IngestionResults: " + ingestionResult.toString());
        } else {
            System.out.println("No IngestionResults available");
        }

    }
}