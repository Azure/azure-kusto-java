import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

public class FileIngestion {

    private static final String appId = "<application Id aka service principal>";
    private static final String appKey = "<application key / secret>";

    public static void main(String[] args) {
        try {
            String kustoClusterPath = "https://ingest-<cluster-name>.kusto.windows.net";
            String dbName = "<databaseName>";
            String tableName = "<tableName>";
            String dataMappingName = "<dataMappingName>";
            String filePath = "<localFilePath>";

            KustoConnectionStringBuilder kcsb = KustoConnectionStringBuilder.createWithAadApplicationCredentials(kustoClusterPath, appId, appKey);
            IngestClient client = IngestClientFactory.createClient(kcsb);

            IngestionProperties ingestionProperties = new IngestionProperties(dbName, tableName);
            ingestionProperties.setJsonMappingName(dataMappingName);

            FileSourceInfo fileSourceInfo = new FileSourceInfo(filePath, 0);
            IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}