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
    public static void main(String[] args) throws IngestionServiceException, URISyntaxException, IngestionClientException, IOException {
        // a();
        System.out.println("finish");
        try {
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithUserPrompt("https://ingest-avdvircluster.westeurope.dev.kusto.windows.net");
            try (IngestClient client = IngestClientFactory.createClient(csb)) {
                IngestionProperties ingestionProperties = new IngestionProperties("ohtst",
                        "t");
                // ingestionProperties.setIngestionMapping("mapy", IngestionMapping.IngestionMappingKind.Csv);
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);
                ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
                ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.QUEUE_AND_TABLE);
                // FileSourceInfo fileSourceInfo = new FileSourceInfo("C:\\Users\\ohbitton\\OneDrive - Microsoft\\Desktop\\big_dataset.csv", 0);
                FileSourceInfo fileSourceInfo = new FileSourceInfo("C:\\Users\\ohbitton\\OneDrive - Microsoft\\Desktop\\data\\bad.csv", 0);
                IngestionResult ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);
                List<IngestionStatus> statuses = ingestionResult.getIngestionStatusCollection();
                Integer timeoutInSec = 400;
                // step 3: poll on the result.
                while ((statuses.get(0).status == OperationStatus.Pending || statuses.get(0).status == OperationStatus.Queued) && timeoutInSec > 0) {
                    Thread.sleep(1000);
                    timeoutInSec -= 1;
                    statuses = ingestionResult.getIngestionStatusCollection();
                }

                ObjectMapper objectMapper = new ObjectMapper();
                JavaTimeModule module = new JavaTimeModule();
                objectMapper.registerModule(module);
                objectMapper.registerModule(new JavaTimeModule());
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
                ingestionProperties2.setDataFormat(IngestionProperties.DataFormat.CSV);
                ingestionProperties2.setIngestionMapping(new ColumnMapping[] {csvColumnMapping, csvColumnMapping2}, IngestionMapping.IngestionMappingKind.CSV);

                IngestionResult ingestionResult2 = client.ingestFromStream(info, ingestionProperties2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void a() throws IOException, IngestionClientException, IngestionServiceException, URISyntaxException {

        String ClientID = "d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66";
        String pass = "d2E7Q~WzIL._3KQqGU9W0vSXNUU4EnNeI4C~r";
        String auth = "72f988bf-86f1-41af-91ab-2d7cd011db47";
        // IngestClient client =
        // IngestClientFactory.createClient(ConnectionStringBuilder.createWithDeviceCodeCredentials("https://ingest-ohbitton.kusto.windows.net"));
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials("https://ingest-ohadprod.westeurope.kusto.windows.net",
                ClientID, pass, auth);

        IngestionProperties ingestionProperties = new IngestionProperties("ohtst", "orcy");
        IngestClient streamclient = IngestClientFactory.createClient(csb);

        ingestionProperties.setDataFormat("csv");
        ByteArrayOutputStream st = new java.io.ByteArrayOutputStream();
        st.write("asd,2".getBytes());
        InputStream byteArrayInputStream = new java.io.ByteArrayInputStream(st.toByteArray());
        // InputStreamReader inputStreamReader = new InputStreamReader(byteArrayInputStream);
        StreamSourceInfo info = new StreamSourceInfo(byteArrayInputStream);
        // info.setCompressionType(CompressionType.gz);
        // ingestionProperties.setIngestionMapping("csv ._- map", IngestionMapping.IngestionMappingKind.csv);
        // ingestionProperties.setIngestByTags(new ArrayList<String>(){{add("tagyTag");}});

        System.out.println("here");
        IngestionResult ingestionResult = streamclient.ingestFromStream(info, ingestionProperties);
    }
}
