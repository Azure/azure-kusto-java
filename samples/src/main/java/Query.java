// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.HttpClientProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Query {
   static  void printVersion( Class clazz) {
        Package p = clazz.getPackage();
        System.out.printf("%s%n  Title: %s%n  Version: %s%n  Vendor: %s%n",
                clazz.getName(),
                p.getImplementationTitle(),
                p.getImplementationVersion(),
                p.getImplementationVendor());
    }
    public static void main(String[] args) {
        printVersion(com.microsoft.aad.msal4j.IAccount.class);
        try {
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithUserPrompt(
                    System.getenv("ENGINE_CONNECTION_STRING"));

            HttpClientProperties properties = HttpClientProperties.builder()
                    .keepAlive(true)
                    .maxKeepAliveTime(120)
                    .maxConnectionsPerRoute(40)
                    .maxConnectionsTotal(40)
                    .build();

            Client client = ClientFactory.createClient(csb, properties);

            KustoOperationResult results = client.execute(".show version");
            KustoOperationResult results2 = client.execute(".show version");
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
