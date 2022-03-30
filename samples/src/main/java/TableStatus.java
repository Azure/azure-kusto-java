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

import static com.microsoft.azure.kusto.ingest.IngestionProperties.IngestionReportMethod.QUEUE_AND_TABLE;

public class TableStatus {
    public static void main(String[] args) {
        try {
            Integer timeoutInSec = Integer.getInteger("timeoutInSec");

            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(System.getProperty("clusterPath"),
                    System.getProperty("appId"),
                    System.getProperty("appKey"),
                    System.getProperty("appTenant"));
            IngestionResult ingestionResult;
            try (IngestClient client = IngestClientFactory.createClient(csb)) {
                IngestionProperties ingestionProperties = new IngestionProperties(System.getProperty("dbName"),
                        System.getProperty("tableName"));
                ingestionProperties.setIngestionMapping(System.getProperty("dataMappingName"), IngestionMapping.IngestionMappingKind.JSON);
                ingestionProperties.setReportMethod(QUEUE_AND_TABLE);
                ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
                FileSourceInfo fileSourceInfo = new FileSourceInfo(System.getProperty("filePath"), 0);
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
