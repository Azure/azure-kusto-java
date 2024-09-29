import com.azure.core.http.ProxyOptions;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import java.net.InetSocketAddress;

public class Proxy {
    public static void main(String[] args) {

        ProxyOptions proxyOptions = new ProxyOptions(ProxyOptions.Type.HTTP,
                InetSocketAddress.createUnresolved("host.example.com", 8080));

        HttpClientProperties providedProperties = HttpClientProperties.builder()
                .proxy(proxyOptions)
                .build();
        try {
            Client client = ClientFactory.createClient(ConnectionStringBuilder.createWithUserPrompt(
                    "https://ohadev.swedencentral.dev.kusto.windows.net"), providedProperties);
            client.executeMgmt(".show version");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
