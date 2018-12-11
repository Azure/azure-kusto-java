import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.Results;

public class Query {

    public static void main(String[] args) {

        try {
            ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                    System.getProperty("clusterPath"),
                    System.getProperty("appId"),
                    System.getProperty("appKey"),
                    System.getProperty("appTenant"));
            ClientImpl client = new ClientImpl(csb);

            Results results = client.execute( System.getProperty("dbName"), System.getProperty("query"));

            System.out.println(String.format("Kusto sent back %s rows.", results.getValues().size()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}