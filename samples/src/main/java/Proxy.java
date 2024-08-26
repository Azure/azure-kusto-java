import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.HttpClientProperties;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import org.apache.http.HttpHost;
import org.apache.http.conn.routing.HttpRoute;

public class Proxy {
    public static void main(String[] args) {
        // See file test\java\com\microsoft\azure\kusto\data\http\SimpleProxyPlanner for an example of a custom route planner class
        HttpClientProperties providedProperties = HttpClientProperties.builder()
                .routePlanner((host, request, context) -> {
                    // Your custom route planning logic here
                    return new HttpRoute(new HttpHost("example.com"));
                }).build();
        try (Client client = ClientFactory.createClient(ConnectionStringBuilder.createWithUserPrompt("https://ohadev.swedencentral.dev.kusto.windows.net"),
                providedProperties)) {
            client.execute(".show version");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
