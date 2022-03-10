// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.*;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.List;

public class FileIngestion {
    public static void main(String[] args) {
        try {
            ConnectionStringBuilder csb =
                    ConnectionStringBuilder.createWithAadApplicationCredentials("https://ingest-ohadprod.westeurope.kusto.windows.net",
                            "d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66",
                            "d2E7Q~WzIL._3KQqGU9W0vSXNUU4EnNeI4C~r",
                            "microsoft.com");
            try (IngestClient client = IngestClientFactory.createClient(csb)) {
                IngestionProperties ingestionProperties = new IngestionProperties("ohtst",
                        "ddd");
//                ingestionProperties.setIngestionMapping("mapy", IngestionMapping.IngestionMappingKind.Csv);
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.csv);
                FileSourceInfo fileSourceInfo = new FileSourceInfo("C:\\Users\\ohbitton\\OneDrive - Microsoft\\Desktop\\big_dataset.csv", 0);
                IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);
                List<IngestionStatus> statuses = ingestionResult.getIngestionStatusCollection();
Integer timeoutInSec = 30;
                // step 3: poll on the result.
                while (statuses.get(0).status == OperationStatus.Pending && timeoutInSec > 0) {
                    Thread.sleep(1000);
                    timeoutInSec -= 1;
                    statuses = ingestionResult.getIngestionStatusCollection();
                }

                ObjectMapper objectMapper = new ObjectMapper();
                JavaTimeModule module = new JavaTimeModule();
                module.addSerializer(Instant.class, new InstantSerializerWithMilliSecondPrecision());
                objectMapper.registerModule(module);                objectMapper.registerModule(new JavaTimeModule());
                String resultAsJson = objectMapper.writeValueAsString(statuses.get(0));

                System.out.println(resultAsJson);
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
                ingestionProperties2.setIngestionMapping(new ColumnMapping[]{csvColumnMapping, csvColumnMapping2}, IngestionMapping.IngestionMappingKind.Csv);

                IngestionResult ingestionResult2 = client.ingestFromStream(info, ingestionProperties2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}