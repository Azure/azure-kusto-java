import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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

            // Ingest From File ASYNC:
            CompletableFuture<IngestionResult> cf = client.ingestFromFileAsync(fileSourceInfo, ingestionProperties);

            try {
                IngestionResult ingestionResult = cf.join();
                System.out.println(ingestionResult.toString());
            } catch (CompletionException e) {
                // The CompletionException includes in its cause the original exception that occurred during the ingestion.
                throw new Exception(e.getCause());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}