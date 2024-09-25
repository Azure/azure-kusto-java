package com.microsoft.azure.kusto.data;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.http.HttpRequestBuilder;
import com.microsoft.azure.kusto.data.http.HttpTracing;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class HeaderTest {

    @Test
    public void testHeadersDefault() throws URISyntaxException, DataClientException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");

        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);

        ClientRequestProperties crp = new ClientRequestProperties();

        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(crp)
                .withRequestPrefix("KJC.execute")
                .withActivitySuffix("QueryCommand")
                .withClientDetails(client.getClientDetails())
                .build();

        HttpRequest request = HttpRequestBuilder
                .newPost("https://www.example.com")
                .withTracing(tracing)
                .build();

        Map<String, String> headers = extractHeadersFromAzureRequest(request);

        Assertions.assertNotNull(headers.get("x-ms-app"));
        Assertions.assertNotNull(headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client"));
    }

    @Test
    public void testHeadersWithCustomCsb() throws URISyntaxException, DataClientException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setApplicationNameForTracing("testApp");
        csb.setUserNameForTracing("testUser");
        csb.setClientVersionForTracing("testVersion");

        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);

        ClientRequestProperties crp = new ClientRequestProperties();

        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(crp)
                .withRequestPrefix("KJC.execute")
                .withActivitySuffix("QueryCommand")
                .withClientDetails(client.getClientDetails())
                .build();

        HttpRequest request = HttpRequestBuilder
                .newPost("https://www.example.com")
                .withTracing(tracing)
                .build();

        Map<String, String> headers = extractHeadersFromAzureRequest(request);

        Assertions.assertEquals("testApp", headers.get("x-ms-app"));
        Assertions.assertEquals("testUser", headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client"));
        Assertions.assertTrue(headers.get("x-ms-client-version").endsWith("|testVersion"));
    }

    @Test
    public void testHeadersWithCustomCsbAndClientRequestProperties() throws URISyntaxException, DataClientException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setApplicationNameForTracing("testApp");
        csb.setUserNameForTracing("testUser");
        csb.setClientVersionForTracing("testVersion");

        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setApplication("crpApp");
        crp.setUser("crpUser");

        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(crp)
                .withRequestPrefix("KJC.execute")
                .withActivitySuffix("QueryCommand")
                .withClientDetails(client.getClientDetails())
                .build();

        HttpRequest request = HttpRequestBuilder
                .newPost("https://www.example.com")
                .withTracing(tracing)
                .build();

        Map<String, String> headers = extractHeadersFromAzureRequest(request);

        Assertions.assertEquals("crpApp", headers.get("x-ms-app"));
        Assertions.assertEquals("crpUser", headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client"));
        Assertions.assertTrue(headers.get("x-ms-client-version").endsWith("|testVersion"));
    }

    @Test
    public void testSetConnectorNameAndVersion() throws URISyntaxException, DataClientException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setConnectorDetails("myConnector", "myVersion", null, null, false, null);

        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);

        ClientRequestProperties crp = new ClientRequestProperties();

        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(crp)
                .withRequestPrefix("KJC.execute")
                .withActivitySuffix("QueryCommand")
                .withClientDetails(client.getClientDetails())
                .build();

        HttpRequest request = HttpRequestBuilder
                .newPost("https://www.example.com")
                .withTracing(tracing)
                .build();

        Map<String, String> headers = extractHeadersFromAzureRequest(request);

        Assertions.assertEquals("[none]", headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client:"));

        Assertions.assertTrue(headers.get("x-ms-app").startsWith("Kusto.myConnector:{myVersion}"));
    }

    @Test
    public void testSetConnectorNoAppVersion() throws URISyntaxException, DataClientException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setConnectorDetails("myConnector", "myVersion", null, null, true, "myApp");

        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);

        ClientRequestProperties crp = new ClientRequestProperties();

        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(crp)
                .withRequestPrefix("KJC.execute")
                .withActivitySuffix("QueryCommand")
                .withClientDetails(client.getClientDetails())
                .build();

        HttpRequest request = HttpRequestBuilder
                .newPost("https://www.example.com")
                .withTracing(tracing)
                .build();

        Map<String, String> headers = extractHeadersFromAzureRequest(request);

        Assertions.assertTrue(headers.get("x-ms-user").length() > 0);
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client:"));

        Assertions.assertTrue(headers.get("x-ms-app").startsWith("Kusto.myConnector:{myVersion}"));
    }

    @Test
    public void testSetConnectorFull() throws URISyntaxException, DataClientException {
        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithAadManagedIdentity("https://testcluster.kusto.windows.net");
        csb.setConnectorDetails("myConnector", "myVersion", "myApp", "myAppVersion", true, "myUser", Pair.of("myField", "myValue"));

        ClientImpl client = (ClientImpl) ClientFactory.createClient(csb);

        ClientRequestProperties crp = new ClientRequestProperties();

        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(crp)
                .withRequestPrefix("KJC.execute")
                .withActivitySuffix("QueryCommand")
                .withClientDetails(client.getClientDetails())
                .build();

        HttpRequest request = HttpRequestBuilder
                .newPost("https://www.example.com")
                .withTracing(tracing)
                .build();

        Map<String, String> headers = extractHeadersFromAzureRequest(request);

        Assertions.assertEquals("myUser", headers.get("x-ms-user"));
        Assertions.assertTrue(headers.get("x-ms-client-version").startsWith("Kusto.Java.Client:"));

        Assertions.assertEquals("Kusto.myConnector:{myVersion}|App.{myApp}:{myAppVersion}|myField:{myValue}", headers.get("x-ms-app"));
    }

    private Map<String, String> extractHeadersFromAzureRequest(HttpRequest request) {
        Map<String, String> uncomplicatedHeaders = new HashMap<>();
        HttpHeaders headers = request.getHeaders();
        headers.forEach(header -> uncomplicatedHeaders.put(header.getName(), header.getValue()));
        return uncomplicatedHeaders;
    }

}