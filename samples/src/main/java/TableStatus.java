import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.KustoIngestClient;
import com.microsoft.azure.kusto.ingest.KustoIngestClientFactory;
import com.microsoft.azure.kusto.ingest.KustoIngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.KustoIngestionResult;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.UUID;

public class TableStatus {
    public static void main(String[] args) throws Exception {

        // step 1: Retrieve table uri
        String applicationClientId = null;
        String applicationKey = null;
        KustoConnectionStringBuilder kcsb = KustoConnectionStringBuilder.createWithAadApplicationCredentials(
                "https://ingest-CLUSTERNAME.kusto.windows.net", applicationClientId, applicationKey);
        KustoIngestClient client = KustoIngestClientFactory.createClient(kcsb);

        // step 2: create an entry in the azure storage table
        String blobUri = "";
        UUID uuid = UUID.randomUUID();
        System.out.println(uuid);
        BlobSourceInfo blob = new BlobSourceInfo(blobUri, 1000L);
        blob.setSourceId(uuid);

        // Now the entry in the table exists.
        // We should now pass it as part of the message to the ingest endpoint.
        String dbName = null;
        String tableName = null;
        KustoIngestionProperties ingestionProperties = new KustoIngestionProperties(dbName, tableName);
        ingestionProperties.setReportLevel(KustoIngestionProperties.IngestionReportLevel.FailuresAndSuccesses);
        ingestionProperties.setReportMethod(KustoIngestionProperties.IngestionReportMethod.Table);
        //IKustoIngestionResult kustoIngestionResult =
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobUri);
        KustoIngestionResult kustoIngestionResult = client.ingestFromBlob(blobSourceInfo, ingestionProperties);
        List<IngestionStatus> statuses = kustoIngestionResult.GetIngestionStatusCollection();

        // step 3: poll on the result.
        while (statuses.get(0).status == OperationStatus.Pending) {
            Thread.sleep(1000);
            statuses = kustoIngestionResult.GetIngestionStatusCollection();
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String resultAsJson = objectMapper.writeValueAsString(statuses.get(0));
        System.out.println(resultAsJson);

    }
}
