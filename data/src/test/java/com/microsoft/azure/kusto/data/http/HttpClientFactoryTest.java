package com.microsoft.azure.kusto.data.http;

import com.microsoft.azure.kusto.data.HttpClientProperties;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HttpClientFactoryTest {
    @Test
    @DisplayName("test create http client from null properties")
    void testNullProperties() {
        Assertions.assertDoesNotThrow(() -> HttpClientFactory.create(null));
    }

    @Test
    @DisplayName("test create http client from properties")
    void testProperties() {
        HttpClientProperties properties = HttpClientProperties.builder().build();
        final CloseableHttpClient httpClient = HttpClientFactory.create(properties);
        Assertions.assertNotNull(httpClient);
    }
}
