// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoResponseDataSetV2;
import com.microsoft.azure.kusto.data.KustoStreamingQueryOptions;
import com.microsoft.azure.kusto.data.StreamingDataTable;
import com.microsoft.azure.kusto.data.WellKnownDataSet;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates how to execute a streaming query and process results in batches of configurable
 * size. This is ideal for the Eventstream connector pattern where you want to read rows in
 * manageable chunks to control memory usage.
 *
 * <p>The Kusto service enforces a maximum response size of 64 MB per query. By streaming the
 * response and processing rows in small batches, you can handle large result sets without
 * exceeding memory limits on the client side.</p>
 *
 * <h3>How to run:</h3>
 * <pre>
 * cd samples
 * mvn clean compile exec:java -Dexec.mainClass="StreamingQueryBatch" \
 *     -DclusterPath="https://yourcluster.region.kusto.windows.net" \
 *     -DappId="app-id" \
 *     -DappKey="appKey" \
 *     -DappTenant="tenant-id" \
 *     -DdbName="dbName" \
 *     -Dquery="YourTable | take 10000" \
 *     -DbatchSize="500"
 * </pre>
 */
public class StreamingQueryBatch {
    public static void main(String[] args) {
        try {
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    System.getProperty("clusterPath"),
                    System.getProperty("appId"),
                    System.getProperty("appKey"),
                    System.getProperty("appTenant"));

            HttpClientProperties properties = HttpClientProperties.builder()
                    .keepAlive(true)
                    .maxConnectionsTotal(40)
                    .build();

            Client client = ClientFactory.createClient(csb, properties);
            String database = System.getProperty("dbName");
            String query = System.getProperty("query", ".show version");
            int batchSize = Integer.getInteger("batchSize", 500);

            ClientRequestProperties crp = new ClientRequestProperties();
            crp.setTimeoutInMilliSec(TimeUnit.MINUTES.toMillis(5));

            // 1. Create streaming options with a batch size.
            //    This configures how many rows nextRows() returns at a time.
            KustoStreamingQueryOptions options = KustoStreamingQueryOptions.withMaxBatchRowCount(batchSize);

            try (KustoResponseDataSetV2 response = client.executeQuery(database, query, crp, options)) {

                // 2. Iterate through tables - find the PrimaryResult
                while (response.hasNextTable()) {
                    StreamingDataTable table = response.getTable();

                    if (table.getTableKind() != WellKnownDataSet.PrimaryResult) {
                        // Skip non-primary tables (QueryProperties, QueryCompletionInformation, etc.)
                        continue;
                    }

                    System.out.printf("Streaming primary result: %s (%d columns)%n",
                            table.getTableName(), table.getColumns().length);

                    // 3. Read rows in batches using nextRows().
                    //    Each call returns up to batchSize rows (configured via options).
                    //    The last batch may contain fewer rows.
                    int batchNumber = 0;
                    int totalRows = 0;

                    while (!table.isExhausted()) {
                        List<List<Object>> batch = table.nextRows();
                        if (batch.isEmpty()) {
                            break;
                        }

                        batchNumber++;
                        totalRows += batch.size();
                        System.out.printf("  Batch %d: %d rows (total so far: %d)%n",
                                batchNumber, batch.size(), totalRows);

                        // Process each row in the batch
                        processBatch(batch);
                    }

                    System.out.printf("Done. Processed %d rows in %d batches of up to %d rows.%n",
                            totalRows, batchNumber, batchSize);
                }

                if (response.hasErrors()) {
                    System.err.println("Query completed with errors.");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Process a batch of rows. In a real connector, this might serialize the batch and
     * forward it to a downstream system (e.g., Eventstream).
     */
    private static void processBatch(List<List<Object>> batch) {
        // Example: just print the first row of each batch
        if (!batch.isEmpty()) {
            System.out.printf("    First row: %s%n", batch.get(0));
        }
    }
}
