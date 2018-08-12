import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.KustoIngestClient;
import com.microsoft.azure.kusto.ingest.KustoIngestionProperties;

public class SampleFileIngestion {

    private static final String appId = "";
    private static final String appKey = "";

    public static void main(String[] args) {
        try {
            String kustoClusterPath = "https://ingest-<cluster name>.kusto.windows.net/";
            String filePath = "";
            String dbName = "";
            String tableName = "";
            String dataMappingName = "";
            String dataFormat = "json";

            KustoConnectionStringBuilder kcsb = KustoConnectionStringBuilder.createWithAadApplicationCredentials(kustoClusterPath,appId,appKey);
            KustoIngestionProperties ingestionProperties = new KustoIngestionProperties(dbName,tableName);
            ingestionProperties.setJsonMappingName(dataMappingName);

            KustoIngestClient client = new KustoIngestClient(kcsb);

            for(int i = 1; i<11; i++){
                client.ingestFromSingleFile(filePath+i+".json", ingestionProperties);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
