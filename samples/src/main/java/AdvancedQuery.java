// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class AdvancedQuery {
    public static void main(String[] args) {

        try {
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    System.getProperty("clusterPath"),
                    System.getProperty("appId"),
                    System.getProperty("appKey"),
                    System.getProperty("appTenant"));

            HttpClientProperties properties = HttpClientProperties.builder()
                    .keepAlive(true)
                    .maxKeepAliveTime(120)
                    .maxConnectionsPerRoute(40)
                    .maxConnectionsTotal(40)
                    .build();

            Client client = ClientFactory.createClient(csb, properties);
            String database = System.getProperty("dbName");
            String newLine = System.getProperty("line.separator");

            // Create a table named Events with 100 rows of data.
            String tableCommand = String.join(newLine,
                    ".set-or-replace Events <|",
                    "range x from 1 to 100 step 1",
                    "| extend ts = totimespan(strcat(x,'.00:00:00'))",
                    "| project timestamp = now(ts), eventName = strcat('event ', x)");
            client.executeMgmt(database, tableCommand);

            // Query for an event where the name is "event 1".
            // Utilize ClientRequestProperties to pass query parameters to protect against injection attacks.
            // We should expect to receive 1 row based on the data we created above.
            ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setParameter("eventNameFilter", "event 1");
            String query = String.join(newLine,
                    "declare query_parameters(eventNameFilter:string);",
                    "Events",
                    "| where eventName == eventNameFilter");
            KustoOperationResult results = client.executeQuery(database, query, clientRequestProperties);
            KustoResultSetTable mainTableResult = results.getPrimaryResults();
            System.out.printf("Kusto sent back %s rows.%n", mainTableResult.count());

            // Iterate values
            List<Event> events = new ArrayList<>();
            while (mainTableResult.next()) {
                events.add(new Event(mainTableResult.getKustoDateTime("timestamp"), mainTableResult.getString("eventName")));
            }

            System.out.println(events.get(0).toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class Event {
        LocalDateTime _timestamp;
        String _eventName;

        public Event(LocalDateTime timestamp, String eventName) {
            _timestamp = timestamp;
            _eventName = eventName;
        }

        public String toString() {
            return String.format("Timestamp: %s, Event Name: %s", _timestamp, _eventName);
        }
    }
}
