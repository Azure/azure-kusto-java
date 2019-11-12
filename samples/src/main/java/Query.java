import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Query {

    public static void main(String[] args) {

        try {
            String ClientID ="d5e0a24c-3a09-40ce-a1d6-dc5ab58dae66";
            String pass = "L+0hoM34kqC22XRniWOgkETwVvawiir2odEjYqZeyXA=";
            String auth = "72f988bf-86f1-41af-91ab-2d7cd011db47";
//            IngestClient client = IngestClientFactory.createClient(ConnectionStringBuilder.createWithDeviceCodeCredentials("https://ingest-ohbitton.kusto.windows.net"));
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials("https://ohbitton.dev.kusto.windows.net",ClientID,pass,auth);

//            csb = ConnectionStringBuilder.createWithDeviceCodeCredentials("https://kuskusdev.kusto.windows.net");
            ClientImpl client = new ClientImpl(csb);
//            clientRequestProperties1.setOption("notruncation", true);

//            System.out.println(String.format("Kusto sent back %s rows.", results.getValues().size()));TODO

            // in case we want to pass client request properties
            ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setTimeoutInMilliSec(TimeUnit.MINUTES.toMillis(1));
            KustoResponseResultSet results = client.execute( "the what", "allDataTypes | where isnotnull(f) | extend ingestion_time() | sort by $IngestionTime", clientRequestProperties);
            KustoResultTable primaryResults = results.getPrimaryResults();
            while(primaryResults.next()){
                ArrayList<Object> currentRow = primaryResults.getCurrentRow();
                System.out.println(currentRow.get(0).toString());
                primaryResults.getDate(5);
                primaryResults.getDate(10);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}