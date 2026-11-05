package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class ClientTest {

    @ParameterizedTest
    @ValueSource(ints = {301, 302, 307, 308})
    void testNoRedirectsClientFail(int code) throws IOException, URISyntaxException {
        AtomicInteger redirectedRequests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            int responseCode;
            if (path.equals("/redirected")) {
                redirectedRequests.incrementAndGet();
                responseCode = 200;
            } else if (path.equals("/v1/rest/auth/metadata")) {
                responseCode = 404;
            } else {
                responseCode = code;
                exchange.getResponseHeaders().set("Location", "/redirected");
            }
            exchange.sendResponseHeaders(responseCode, -1);
            exchange.close();
        });
        server.start();
        try {
            String cluster = "http://127.0.0.1:" + server.getAddress().getPort();
            Client client = ClientFactory.createClient(ConnectionStringBuilder.createWithAadAccessTokenAuthentication(cluster, "test-token"));
            DataServiceException exception = Assertions.assertThrows(DataServiceException.class, () -> client.executeQuery("db", "table"));
            Assertions.assertTrue(exception.getMessage().contains(Integer.toString(code)));
            Assertions.assertFalse(exception.getMessage().contains("metadata"));
            Assertions.assertEquals(0, redirectedRequests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("test url parsing")
    void testUrlParsing() throws URISyntaxException {
        Map<String, String> tests = new HashMap<>();
        tests.put("https://kusto.test.com", "https://kusto.test.com");
        tests.put("https://kusto.test.com/", "https://kusto.test.com");
        tests.put("https://kusto.test.com/test", "https://kusto.test.com/test");
        tests.put("https://kusto.test.com:4242", "https://kusto.test.com:4242");
        tests.put("https://kusto.test.com:4242/", "https://kusto.test.com:4242");
        tests.put("https://kusto.test.com:4242/test", "https://kusto.test.com:4242/test");
        tests.put("https://kusto.test.com;fed=true", "https://kusto.test.com");
        tests.put("https://kusto.test.com/;fed=true", "https://kusto.test.com");
        tests.put("https://kusto.test.com/test;fed=true", "https://kusto.test.com/test");
        tests.put("https://kusto.test.com:4242;fed=true", "https://kusto.test.com:4242");
        tests.put("https://kusto.test.com:4242/;fed=true", "https://kusto.test.com:4242");
        tests.put("https://kusto.test.com:4242/test;fed=true", "https://kusto.test.com:4242/test");
        tests.put("https://ade.loganalytics.io/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups/some_resource_group/providers/microsoft" +
                ".operationalinsights/workspaces/some_workspace",
                "https://ade.loganalytics.io/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups/some_resource_group/providers/microsoft" +
                        ".operationalinsights/workspaces/some_workspace");
        tests.put("https://ade.loganalytics.io/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups/some_resource_group/providers/microsoft" +
                ".operationalinsights/workspaces/some_workspace/",
                "https://ade.loganalytics.io/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65" +
                        "/resourcegroups/some_resource_group/providers/microsoft" +
                        ".operationalinsights/workspaces/some_workspace");
        tests.put("https://ade.loganalytics.io:4242/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups/some_resource_group/providers/microsoft" +
                ".operationalinsights/workspaces/some_workspace/",
                "https://ade.loganalytics.io:4242/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65" +
                        "/resourcegroups/some_resource_group/providers/microsoft" +
                        ".operationalinsights/workspaces/some_workspace");
        tests.put("https://ade.loganalytics.io:4242/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups/some_resource_group/providers/microsoft" +
                ".operationalinsights/workspaces/some_workspace/;fed=true",
                "https://ade.loganalytics.io:4242/subscriptions/da45f7ac-97c0-4fff-ac66" +
                        "-3b6810eb4f65" +
                        "/resourcegroups/some_resource_group/providers/microsoft" +
                        ".operationalinsights/workspaces/some_workspace");
        tests.put("https://kusto.aria.microsoft.com", "https://kusto.aria.microsoft.com");
        tests.put("https://kusto.aria.microsoft.com/", "https://kusto.aria.microsoft.com");
        tests.put("https://kusto.aria.microsoft.com/;fed=true", "https://kusto.aria.microsoft.com");

        for (Map.Entry<String, String> entry : tests.entrySet()) {
            ClientImpl client = new ClientImpl(ConnectionStringBuilder.createWithAadAccessTokenAuthentication(entry.getKey(), "test"));
            Assertions.assertEquals(entry.getValue(), client.getClusterUrl());
        }
    }
}
