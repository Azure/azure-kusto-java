import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.KustoIngestClient;
import com.microsoft.azure.kusto.ingest.KustoIngestClientFactory;
import com.microsoft.azure.kusto.ingest.KustoIngestionProperties;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

public class FileIngestion {

    private static final String appId = "2cb0cc4c-c9be-4301-b2aa-718935b0ce1d";
    private static final String appKey = "11hZc+sXY7cwFQ91DMmjEvPFfBHxN8P25kV+BH8A9qg="; //2cb0cc4c-c9be-4301-b2aa-718935b0ce1d

    public static void main(String[] args) {
        try {
            String kustoClusterPath = "https://ingest-csetests.westeurope.kusto.windows.net";
            String dbName = "raabusal";
            String tableName = "test1";
            String dataMappingName = "map1";
            String dataFormat = "json";

            KustoConnectionStringBuilder kcsb = KustoConnectionStringBuilder.createWithAadApplicationCredentials(kustoClusterPath,appId,appKey);
            KustoIngestionProperties ingestionProperties = new KustoIngestionProperties(dbName,tableName);
            ingestionProperties.setJsonMappingName(dataMappingName);

            KustoIngestClient client = KustoIngestClientFactory.createClient(kcsb);

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
