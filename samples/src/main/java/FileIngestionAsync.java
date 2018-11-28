import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

import java.io.File;
import java.util.concurrent.CompletableFuture;

public class FileIngestionAsync {
    private static final String appId = "2cb0cc4c-c9be-4301-b2aa-718935b0ce1d";
    private static final String appKey = "hdQItZsJxEIwYUJetEbhqhbp7stFIuXz4ezlCBQNqVg=";

    public static void main(String[] args) {
        try {
            String kustoClusterPath = "https://ingest-csetests.westeurope.kusto.windows.net";
            String dbName = "raabusal";
            String tableName = "test1";
            String dataMappingName = "map1";
            String dataFormat = "json";

            ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(kustoClusterPath,appId,appKey,"72f988bf-86f1-41af-91ab-2d7cd011db47");
            IngestionProperties ingestionProperties = new IngestionProperties(dbName,tableName);
            ingestionProperties.setJsonMappingName(dataMappingName);

            IngestClient client = IngestClientFactory.createClient(kcsb);

            String filePath = "C:\\Development\\temp\\kusto test data\\testdata1.json";
            FileSourceInfo fileSourceInfo = new FileSourceInfo(filePath, (new File(filePath)).length());

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
            CompletableFuture<Boolean> cf3 = cf2.thenApply(FileIngestionAsync::doSomethingWithIngestionResultReturnBool);


            IngestionResult result = cf2.get();
            System.out.println("IngestionResult: " + result.toString());

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

    private static boolean doSomethingWithIngestionResultReturnBool(IngestionResult ingestionResult){
        if(ingestionResult != null) {
            System.out.println("IngestionResults: " + ingestionResult.toString());
            return true;
        } else {
            System.out.println("No IngestionResults available");
            return false;
        }
    }
}