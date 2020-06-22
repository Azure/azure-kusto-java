// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.*;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
            ingestionProperties2.setDataFormat("Csv");
            ingestionProperties2.setIngestionMapping(new ColumnMapping[]{csvColumnMapping, csvColumnMapping2}, IngestionMapping.IngestionMappingKind.Csv);

            IngestionResult ingestionResult2 = client.ingestFromStream(info, ingestionProperties2);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}