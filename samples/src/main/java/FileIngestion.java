import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

public class FileIngestion {

    private static final String appId = "";
    private static final String appKey = "";

    public static void main(String[] args) {
        try {
            String kustoClusterPath = "https://ingest-<cluster-name>.kusto.windows.net";
            String dbName = "";
            String tableName = "";
            String dataMappingName = "";
            String dataFormat = "json";

            KustoConnectionStringBuilder kcsb = KustoConnectionStringBuilder.createWithAadApplicationCredentials(kustoClusterPath,appId,appKey);
            IngestionProperties ingestionProperties = new IngestionProperties(dbName,tableName);
            ingestionProperties.setJsonMappingName(dataMappingName);

            IngestClient client = IngestClientFactory.createClient(kcsb);

            String filePath = "C:\\Development\\temp\\kusto test data\\testdata";

            for(int i = 1; i<11; i++){
                FileSourceInfo fileSourceInfo = new FileSourceInfo(filePath+i+".json", 0);
                client.ingestFromFile(fileSourceInfo, ingestionProperties);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
