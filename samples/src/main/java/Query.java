// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.*;

import java.util.concurrent.TimeUnit;

public class Query {

    public static void main(String[] args) {

        try {
            String ClientID ="d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66";
            String pass = "Unh581Hzn50LRWPW5XTMV-3_j5.DdisG3.";
            String auth = "microsoft.com";
//            IngestClient  client = IngestClientFactory.createClient(ConnectionStringBuilder.createWithDeviceCodeCredentials("https://ingest-ohbitton.kusto.windows.net"));
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials("https://ohbitton.dev.kusto.windows.net/", ClientID, pass, auth);

            ClientImpl client = new ClientImpl(csb);
            ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setOption("ClientRequestId", "ohadId");
            clientRequestProperties.setTimeoutInMilliSec(999999L);
            KustoOperationResult results = client.execute( "ohtst", "ddd | take 1",clientRequestProperties);
            KustoResultSetTable mainTableResult = results.getPrimaryResults();
            System.out.println(String.format("Kusto sent back %s rows.", mainTableResult.count()));

            // in case we want to pass client request properties


            results = client.execute( System.getProperty("dbName"), System.getProperty("query"), clientRequestProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}