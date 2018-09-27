import com.microsoft.azure.kusto.data.DataClientImpl;
import com.microsoft.azure.kusto.data.DataConnectionStringBuilder;
import com.microsoft.azure.kusto.data.DataResults;

public class Query {

    public static void main(String[] args) {

        String appId = "<app id>";
        String appKey = "<app key>";
        String appTenant = "<app tenant id or domain name>";

        String kustoClusterPath = "https://help.kusto.windows.net";
        String dbName = "Samples";

        String query = "StormEvents | take 10";

        try {
            DataConnectionStringBuilder dcsb = DataConnectionStringBuilder.createWithAadApplicationCredentials(kustoClusterPath, appId, appKey, appTenant);
            DataClientImpl client = new DataClientImpl(dcsb);

            DataResults results = client.execute(dbName, query);

            System.out.println(String.format("Kusto sent back %s rows.", results.getValues().size()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
