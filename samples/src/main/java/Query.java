// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Query {
    public static void main(String[] args) {

        try {
            ConnectionStringBuilder csb =
                    ConnectionStringBuilder.createWithAadApplicationCredentials("https://ingest-ohadprod.westeurope.kusto.windows.net",
                            "d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66",
                            "d2E7Q~WzIL._3KQqGU9W0vSXNUU4EnNeI4C~r",
                            "microsoft.com");
            ClientImpl client = new ClientImpl(csb);

            KustoOperationResult results = client.execute("ohtst","d");
            KustoResultSetTable mainTableResult = results.getPrimaryResults();
            System.out.printf("Kusto sent back %s rows.%n", mainTableResult.count());

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