import com.microsoft.azure.kusto.data.connection.DataConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
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
        DataConnectionStringBuilder dcsb = DataConnectionStringBuilder.createWithAadApplicationCredentials(
                "https://ingest-CLUSTERNAME.kusto.windows.net", applicationClientId, applicationKey);
        IngestClient client = IngestClientFactory.createClient(dcsb);

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
        IngestionProperties ingestionProperties = new IngestionProperties(dbName, tableName);
        ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses);
        ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.Table);
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobUri);
        IngestionResult ingestionResult = client.ingestFromBlob(blobSourceInfo, ingestionProperties);
        List<IngestionStatus> statuses = ingestionResult.GetIngestionStatusCollection();

        // step 3: poll on the result.
        while (statuses.get(0).status == OperationStatus.Pending) {
            Thread.sleep(1000);
            statuses = ingestionResult.GetIngestionStatusCollection();
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String resultAsJson = objectMapper.writeValueAsString(statuses.get(0));
        System.out.println(resultAsJson);

    }
}
