import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.HttpClientProperties;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.http.SimpleProxyPlanner;

public class Proxy {
    public static void main(String[] args) {
        HttpClientProperties providedProperties = HttpClientProperties.builder().routePlanner(new SimpleProxyPlanner("localhost", 8080,"http", ".*kusto.windows.*")).build();
        try (Client client = ClientFactory.createClient(ConnectionStringBuilder.createWithUserPrompt("https://ohadev.swedencentral.dev.kusto.windows.net"), providedProperties)) {
            client.execute(".show version");

        } catch (Exception e) {
        e.printStackTrace();
    }
    }
}
