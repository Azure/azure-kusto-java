import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

public class FileIngestion {

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
            ingestionProperties.setIngestionMapping(System.getProperty("dataMappingName"), IngestionMapping.IngestionMappingKind.json);

            FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"), 0);
            IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}