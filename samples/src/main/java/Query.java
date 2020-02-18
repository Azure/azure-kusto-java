import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.Results;

import java.util.concurrent.TimeUnit;

public class Query {

    public static void main(String[] args) {
//
//        try {
//            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
//                    "https://ohbitton.dev.kusto.windows.net",
//                    "d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66",
//                    "L+0hoM34kqC22XRniWOgkETwVvawiir2odEjYqZeyXA=");
//            ClientImpl client = new ClientImpl(csb);
//
//            KustoResponseResults results = client.execute( ("ohtst"), ("TestTable2 | where ColA has 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"));
//            KustoResultSetTable mainTableResultSet = results.getPrimaryResults();
//            mainTableResultSet
//            while (mainTableResultSet.next())
//            {
//                System.out.println(mainTableResultSet.getCurrentRow().get(0));
//                System.out.println(mainTableResultSet.getString(1));
//            }
//            System.out.println(String.format("Kusto sent back %s rows.", mainTableResultSet.count()));
//
//            // in case we want to pass client request properties
//            ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
//            clientRequestProperties.setTimeoutInMilliSec(TimeUnit.MINUTES.toMillis(1));
//
//            results = client.execute( System.getProperty("dbName"), System.getProperty("query"), clientRequestProperties);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }
}