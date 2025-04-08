// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.azure.data.tables.TableAsyncClient;
import com.azure.data.tables.models.TableEntity;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.ColumnMapping;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.kusto.ingest.utils.TableWithSas;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class FileIngestion {
    public static void main(String[] args) {
        try {
            // TableAsyncClient tableAsyncClient =
            // TableWithSas.createTableClientFromUrl("https://5s8kstrldruthruth01.blob.core.windows.net/20230313-ingestdata-e5c334ee145d4b4-0?sv=2018-03-28&sr=c&sig=QshIuU9ZZ1jvcgcPMnHcr0EvCwO9sxZbvAUaAtI%3D&st=2023-03-13T13%3A16%3A57Z&se=2023-03-17T14%3A16%3A57Z&sp=rw",
            // null);
            // tableAsyncClient.createEntity(new TableEntity("123", "123")).block();

            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithUserPrompt("https://ruthruth.eastus.kusto.windows.net");
            try (IngestClient client = IngestClientFactory.createClient(csb)) {
                IngestionProperties ingestionProperties = new IngestionProperties("db2", "TestTable");
                ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
                ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
                FileSourceInfo fileSourceInfo = new FileSourceInfo("C:\\Users\\ohbitton\\OneDrive - Microsoft\\Desktop\\data\\a.csv");
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
                ingestionProperties2.setDataFormat(IngestionProperties.DataFormat.CSV);
                ingestionProperties2.setIngestionMapping(new ColumnMapping[] {csvColumnMapping, csvColumnMapping2}, IngestionMapping.IngestionMappingKind.CSV);

                IngestionResult ingestionResult2 = client.ingestFromStream(info, ingestionProperties2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
