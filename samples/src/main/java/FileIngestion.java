import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.*;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

import java.util.HashMap;

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
            ingestionProperties.setIngestionMapping(System.getProperty("dataMappingName"), IngestionMapping.IngestionMappingKind.Json);

            FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"), 0);
            IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);

            // Ingest with inline ingestion mapping - less recommended
            IngestionProperties ingestionProperties2 = new IngestionProperties(System.getProperty("dbName"),
                    System.getProperty("tableName"));
            ColumnMapping csvColumnMapping = new ColumnMapping("ColA", "string", new HashMap<String,String>(){{
                put(MappingConst.ORDINAL.name(), "0");
            }});
            ColumnMapping csvColumnMapping2 = new ColumnMapping("ColB", "int", new HashMap<String,String>(){{
                put(MappingConst.ORDINAL.name(), "1");
            }});
            ingestionProperties2.setDataFormat("Csv");
            ingestionProperties2.setIngestionMapping(new ColumnMapping[]{csvColumnMapping, csvColumnMapping2}, IngestionMapping.IngestionMappingKind.Csv);
            ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties2);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}