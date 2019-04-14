import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;

import static com.microsoft.azure.kusto.ingest.IngestionProperties.IngestionReportMethod.QueueAndTable;

public class TableStatus {
    public static void main(String[] args) {
        try {

            Integer timeoutInSec = Integer.getInteger("timeoutInSec");

            ConnectionStringBuilder csb =
                    ConnectionStringBuilder.createWithAadApplicationCredentials( System.getProperty("clusterPath"),
                            System.getProperty("appId"),
                            System.getProperty("appKey"),
                            System.getProperty("appTenant"));
            IngestClient client = IngestClientFactory.createClient(csb);

            IngestionProperties ingestionProperties = new IngestionProperties( System.getProperty("dbName"),
                    System.getProperty("tableName"));
            ingestionProperties.setJsonMappingName(System.getProperty("dataMappingName"));
            ingestionProperties.setReportMethod(QueueAndTable);
            ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses);
            FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"), 0);
            IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);
            List<IngestionStatus> statuses = ingestionResult.getIngestionStatusCollection();

            // step 3: poll on the result.
            while (statuses.get(0).status == OperationStatus.Pending && timeoutInSec > 0) {
                Thread.sleep(1000);
                timeoutInSec -= 1;
                statuses = ingestionResult.getIngestionStatusCollection();
            }

            ObjectMapper objectMapper = new ObjectMapper();
            String resultAsJson = objectMapper.writeValueAsString(statuses.get(0));
            System.out.println(resultAsJson);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}