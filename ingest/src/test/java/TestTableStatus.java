import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.*;
import org.codehaus.jackson.map.ObjectMapper;

public class TestTableStatus {
    public static void main(String[] args) throws Exception {

        // step 1: Retrieve table uri
        String applicationClientId = null;
        String applicationKey = null;
        KustoConnectionStringBuilder kcsb = KustoConnectionStringBuilder.createWithAadApplicationCredentials(
                "https://ingest-CLUSTERNAME.kusto.windows.net", applicationClientId, applicationKey);
        KustoIngestClient client = new KustoIngestClient(kcsb);

        // step 2: create an entry in the azure storage table
        String blobUri = "";
        UUID uuid = UUID.randomUUID();
        System.out.println(uuid);
        BlobDescription blob = new BlobDescription(blobUri, 1000L);
        blob.setSourceId(uuid);

        // Now the entry in the table exists.
        // We should now pass it as part of the message to the ingest endpoint.
        String dbName = null;
        String tableName = null;
        KustoIngestionProperties ingestionProperties = new KustoIngestionProperties(dbName, tableName);
        ingestionProperties.setReportLevel(KustoIngestionProperties.IngestionReportLevel.FailuresAndSuccesses);
        ingestionProperties.setReportMethod(KustoIngestionProperties.IngestionReportMethod.Table);
        IKustoIngestionResult kustoIngestionResult = client.ingestFromMultipleBlobs(Collections.singletonList(blob),
                false, ingestionProperties);
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
