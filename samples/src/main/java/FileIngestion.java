// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.ColumnMapping;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class FileIngestion {
    public static void main(String[] args) {
        try {
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(System.getProperty("clusterPath"),
                    System.getProperty("appId"),
                    System.getProperty("appKey"),
                    System.getProperty("appTenant"));
            try (IngestClient client = IngestClientFactory.createClient(csb)) {
                IngestionProperties ingestionProperties = new IngestionProperties(System.getProperty("dbName"),
                        System.getProperty("tableName"));
                ingestionProperties.setIngestionMapping(System.getProperty("dataMappingName"), IngestionMapping.IngestionMappingKind.Json);

                FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"), 0);
                IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);
                ByteArrayOutputStream st = new ByteArrayOutputStream();
                st.write("asd,2".getBytes());
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(st.toByteArray());
                StreamSourceInfo info = new StreamSourceInfo(byteArrayInputStream);

                // Ingest with inline ingestion mapping - less recommended
                IngestionProperties ingestionProperties2 = new IngestionProperties(System.getProperty("dbName"),
                        System.getProperty("tableName"));
                ColumnMapping csvColumnMapping = new ColumnMapping("ColA", "string");
                csvColumnMapping.setOrdinal(0);
                ColumnMapping csvColumnMapping2 = new ColumnMapping("ColB", "int");
                csvColumnMapping2.setOrdinal(1);
                ingestionProperties2.setDataFormat(IngestionProperties.DataFormat.csv);
                ingestionProperties2.setIngestionMapping(new ColumnMapping[] {csvColumnMapping, csvColumnMapping2}, IngestionMapping.IngestionMappingKind.Csv);

                IngestionResult ingestionResult2 = client.ingestFromStream(info, ingestionProperties2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
