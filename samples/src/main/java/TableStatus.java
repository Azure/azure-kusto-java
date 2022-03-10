// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;

import java.util.List;

import static com.microsoft.azure.kusto.ingest.IngestionProperties.IngestionReportMethod.QueueAndTable;

public class TableStatus {
    public static void main(String[] args) {
        try {
            Integer timeoutInSec = 60000;

            ConnectionStringBuilder csb =
                    ConnectionStringBuilder.createWithAadApplicationCredentials("https://ingest-ohadprod.westeurope.kusto.windows.net",
                            "d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66",
                            "d2E7Q~WzIL._3KQqGU9W0vSXNUU4EnNeI4C~r",
                            "microsoft.com");
            IngestionResult ingestionResult;
            try (IngestClient client = IngestClientFactory.createClient(csb)) {
                IngestionProperties ingestionProperties = new IngestionProperties("ohtst",
                        "ddd");
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.csv);

                ingestionProperties.setReportMethod(QueueAndTable);
                ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses);
                FileSourceInfo fileSourceInfo = new FileSourceInfo("C:\\Users\\ohbitton\\OneDrive - Microsoft\\Desktop\\big_dataset.csv", 0);
                ingestionResult = client.ingestFromFile(fileSourceInfo, ingestionProperties);
            }
            List<IngestionStatus> statuses = ingestionResult.getIngestionStatusCollection();

            // step 3: poll on the result.
            while (statuses.get(0).status == OperationStatus.Pending && timeoutInSec > 0) {
                Thread.sleep(1000);
                timeoutInSec -= 1;
                statuses = ingestionResult.getIngestionStatusCollection();
            }

            ObjectMapper objectMapper = new ObjectMapper();

           // Print date nicely
            objectMapper.findAndRegisterModules();
            objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

            String resultAsJson = objectMapper.writeValueAsString(statuses.get(0));
            System.out.println(resultAsJson);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}