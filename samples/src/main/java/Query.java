import com.microsoft.azure.kusto.data.KustoClient;
import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;
import com.microsoft.azure.kusto.data.KustoResults;

public class Query {

    public static void main(String[] args) {

        String appId = "<app id>";
        String appKey = "<app key>";

        String kustoClusterPath = "https://help.kusto.windows.net";
        String dbName = "Samples";

        String query = "StormEvents | take 10";

        try {
            KustoConnectionStringBuilder kcsb = KustoConnectionStringBuilder.createWithAadApplicationCredentials(kustoClusterPath, appId, appKey);
            KustoClient client = new KustoClient(kcsb);

            KustoResults results = client.execute(dbName, query);

            System.out.println(String.format("Kusto sent back %s rows.", results.getValues().size()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
