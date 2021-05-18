// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Query {
    public static void main(String[] args) {

        try {

            ConnectionStringBuilder engineKcsb = ConnectionStringBuilder.createWithUserPrompt("https://kuskusprod" +
                    ".kusto.windows.net");
            Client client1 = ClientFactory.createClient(engineKcsb);
//            client1.execute("KusKus", "KustoLogs | ");

                    client1.execute("KusKus", "KustoLogs\n| where Timestamp between (datetime(2021-04-21T11:40:28)" +
                    "..2h)\n| where Source !has \"MANAGE\" | where SourceId == \"6343AB81\" \n| extend queues = " +
                    "todynamic(extract(\"TracePushQueueDetails:([^\\\\]]+\\\\])\", 1, EventText))");

            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    System.getProperty("clusterPath"),
                    System.getProperty("appId"),
                    System.getProperty("appKey"),
                    System.getProperty("appTenant"));
            ClientImpl client = new ClientImpl(csb);

            KustoOperationResult results = client.execute(System.getProperty("dbName"), System.getProperty("query"));
            KustoResultSetTable mainTableResult = results.getPrimaryResults();
            System.out.println(String.format("Kusto sent back %s rows.", mainTableResult.count()));

            // iterate values
            while (mainTableResult.next()) {
                List<Object> nextValue = mainTableResult.getCurrentRow();
            }

            // in case we want to pass client request properties
            ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setTimeoutInMilliSec(TimeUnit.MINUTES.toMillis(1));

            results = client.execute(System.getProperty("dbName"), System.getProperty("query"), clientRequestProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}