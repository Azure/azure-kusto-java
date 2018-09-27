import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.Results;

public class Query {

    public static void main(String[] args) {

        String appId = "<app id>";
        String appKey = "<app key>";
        String appTenant = "<app tenant id or domain name>";

        String kustoClusterPath = "https://help.kusto.windows.net";
        String dbName = "Samples";

        String query = "StormEvents | take 10";

        try {
            ConnectionStringBuilder dcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(kustoClusterPath, appId, appKey, appTenant);
            ClientImpl client = new ClientImpl(dcsb);

            Results results = client.execute(dbName, query);

            System.out.println(String.format("Kusto sent back %s rows.", results.getValues().size()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
