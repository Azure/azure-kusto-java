package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class ClientTest {

    @Test
    @DisplayName("test url parsing")
    void testUrlParsing() throws URISyntaxException {
        Map<String, String> tests = new HashMap<>();
        tests.put("https://kusto.test.com", "https://kusto.test.com");
        tests.put("https://kusto.test.com/", "https://kusto.test.com");
        tests.put("https://kusto.test.com/test", "https://kusto.test.com");
        tests.put("https://kusto.test.com:4242", "https://kusto.test.com:4242");
        tests.put("https://kusto.test.com:4242/", "https://kusto.test.com:4242");
        tests.put("https://kusto.test.com:4242/test", "https://kusto.test.com:4242");
        tests.put("https://kusto.test.com;fed=true", "https://kusto.test.com");
        tests.put("https://kusto.test.com/;fed=true", "https://kusto.test.com");
        tests.put("https://kusto.test.com/test;fed=true", "https://kusto.test.com");
        tests.put("https://kusto.test.com:4242;fed=true", "https://kusto.test.com:4242");
        tests.put("https://kusto.test.com:4242/;fed=true", "https://kusto.test.com:4242");
        tests.put("https://kusto.test.com:4242/test;fed=true", "https://kusto.test.com:4242");

        for (Map.Entry<String, String> entry : tests.entrySet()) {
            ClientImpl client = new ClientImpl(ConnectionStringBuilder.createWithAadAccessTokenAuthentication(entry.getKey(), "test"));
            Assertions.assertEquals(entry.getValue(), client.getClusterUrl());
        }
    }
}
