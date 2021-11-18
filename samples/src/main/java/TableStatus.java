// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.fasterxml.jackson.databind.ObjectMapper;
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
            Integer timeoutInSec = Integer.getInteger("timeoutInSec");
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    "https://ingest-ohadprod.westeurope.kusto.windows.net",
                    "d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66",
                    "-f90cR6sr-hFC3WBm5ANXtm521_W~ah~Ia",
                    "microsoft.com");
            IngestionResult ingestionResult;
            try (IngestClient client = IngestClientFactory.createClient(csb)) {
                IngestionProperties ingestionProperties = new IngestionProperties("ohtst",
                        "TestTable2");
                ingestionProperties.setReportMethod(QueueAndTable);
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.csv);
                FileSourceInfo fileSourceInfo = new FileSourceInfo("C:\\Users\\ohbitton\\OneDrive - Microsoft\\Desktop\\data\\data.csv", 0);

                ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FailuresAndSuccesses);
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
            String resultAsJson = objectMapper.writeValueAsString(statuses.get(0));
            System.out.println(resultAsJson);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}