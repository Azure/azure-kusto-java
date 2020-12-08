// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

import com.microsoft.azure.kusto.data.*;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Query {

    public static void main(String[] args) {

        try {
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    System.getProperty("clusterPath"),
                    System.getProperty("appId"),
                    System.getProperty("appKey"),
                    System.getProperty("appTenant"));
            ClientImpl client = new ClientImpl(csb);

            KustoOperationResult results = client.execute( System.getProperty("dbName"), System.getProperty("query"));
            KustoResultSetTable mainTableResult = results.getPrimaryResults();
            System.out.println(String.format("Kusto sent back %s rows.", mainTableResult.count()));

            // iterate values
            while(mainTableResult.next()){
                ArrayList<Object> nextValue = mainTableResult.getCurrentRow();
            }

            // in case we want to pass client request properties
            ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setTimeoutInMilliSec(TimeUnit.MINUTES.toMillis(1));

            results = client.execute( System.getProperty("dbName"), System.getProperty("query"), clientRequestProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}