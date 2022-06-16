package com.microsoft.azure.kusto.data;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HttpClientFactoryTest {

    final HttpClientFactory httpClientFactory = HttpClientFactory.getInstance();

    @Test
    @DisplayName("test create http client from null properties")
    void testNullProperties() {
        Assertions.assertDoesNotThrow(() -> httpClientFactory.create(null));
    }

    @Test
    @DisplayName("test create http client from properties")
    void testProperties() {
        HttpClientProperties properties = HttpClientProperties.builder().build();
        final CloseableHttpClient httpClient = httpClientFactory.create(properties);
        Assertions.assertNotNull(httpClient);
    }
}
