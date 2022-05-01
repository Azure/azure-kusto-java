package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import org.apache.commons.lang3.StringUtils;
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
