package com.microsoft.azure.kusto.data.http;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class ProxyPlannerTest {
    @Test
    @DisplayName("test SimpleProxyPlanner")
    void testNullProperties() {
        SimpleProxyPlanner simpleProxyPlanner = new SimpleProxyPlanner("localhost", 8080, "http", ".*kusto.windows.*");
        HttpHost httpHost = simpleProxyPlanner.determineProxy(new HttpHost("https://ohadev.swedencentral.dev.kusto.windows.net"), null, null);
        assert httpHost == null;

        HttpHost httpHost1 = simpleProxyPlanner.determineProxy(new HttpHost("https://kustotest.blob.core.windows.net/container/blob.csv"), null, null);
        new HttpHost("localhost", 8080, "http");
        Assertions.assertEquals(httpHost1, new HttpHost("localhost", 8080, "http"));
    }
}
