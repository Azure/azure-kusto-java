// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoResponseDataSetV2;
import com.microsoft.azure.kusto.data.KustoResultColumn;
import com.microsoft.azure.kusto.data.KustoStreamingQueryOptions;
import com.microsoft.azure.kusto.data.StreamingDataTable;
import com.microsoft.azure.kusto.data.WellKnownDataSet;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates how to execute a streaming query against Kusto and process results row-by-row
 * without loading the entire result set into memory.
 *
 * <p>This is useful when querying large tables (e.g., with a cursor) where loading all rows at
 * once would cause out-of-memory issues.</p>
 *
 * <h3>How to run:</h3>
 * <pre>
 * cd samples
 * mvn clean compile exec:java -Dexec.mainClass="StreamingQuery" \
 *     -DclusterPath="https://yourcluster.region.kusto.windows.net" \
 *     -DappId="app-id" \
 *     -DappKey="appKey" \
 *     -DappTenant="tenant-id" \
 *     -DdbName="dbName" \
 *     -Dquery="YourTable | take 1000"
 * </pre>
 */
public class StreamingQuery {
    public static void main(String[] args) {
        try {
            String appId = System.getProperty("appId");
            String appKey = System.getProperty("appKey");
            String appTenant = System.getProperty("appTenant");
            String clusterPath = System.getProperty("clusterPath");
            boolean useAppKeys = appId != null && appKey != null && appTenant != null;

            // 1. Build connection string
            ConnectionStringBuilder csb = useAppKeys ? ConnectionStringBuilder.createWithAadApplicationCredentials(
                    clusterPath,
                    appId,
                    appKey,
                    appTenant) : ConnectionStringBuilder.createWithAzureCli(clusterPath);

            HttpClientProperties properties = HttpClientProperties.builder()
                    .keepAlive(true)
                    .maxConnectionsTotal(40)
                    .build();

            Client client = ClientFactory.createClient(csb, properties);
            String database = System.getProperty("dbName");
            String query = System.getProperty("query", ".show version");

            // 2. Execute query with streaming - pass KustoStreamingQueryOptions to get a
            //    KustoResponseDataSetV2 that streams the V2 response incrementally
            ClientRequestProperties crp = new ClientRequestProperties();
            crp.setTimeoutInMilliSec(TimeUnit.MINUTES.toMillis(5));

            try (KustoResponseDataSetV2 response = client.executeQuery(
                    database, query, crp, KustoStreamingQueryOptions.create())) {

                System.out.printf("Response version: %s%n", response.getVersion());

                // 3. Iterate through tables in the response
                while (response.hasNextTable()) {
                    StreamingDataTable table = response.getTable();

                    System.out.printf("%nTable: %s (Kind: %s, Id: %d)%n",
                            table.getTableName(), table.getTableKind(), table.getTableId());

                    // Print column headers
                    KustoResultColumn[] columns = table.getColumns();
                    for (KustoResultColumn col : columns) {
                        System.out.printf("  Column: %s (%s)%n", col.getColumnName(), col.getColumnType());
                    }

                    // 4. Stream rows one at a time - no buffering of the full result set
                    int rowCount = 0;
                    while (table.hasNextRow()) {
                        List<Object> row = table.nextRow();
                        rowCount++;

                        // Print first 5 rows as example
                        if (rowCount <= 5) {
                            System.out.printf("  Row %d: %s%n", rowCount, row);
                        }
                    }

                    System.out.printf("  Total rows: %d%n", rowCount);
                }

                // 5. Check for errors after all tables have been read
                if (response.hasErrors()) {
                    System.err.println("Query completed with errors:");
                    for (RuntimeException error : response.getOneApiErrors()) {
                        System.err.println("  " + error.getMessage());
                    }
                }
                if (response.isCancelled()) {
                    System.err.println("Query was cancelled.");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
