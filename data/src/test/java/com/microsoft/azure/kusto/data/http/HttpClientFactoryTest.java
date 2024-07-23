package com.microsoft.azure.kusto.data.http;

import com.azure.core.http.HttpClient;
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
        final HttpClient httpClient = HttpClientFactory.create(properties);
        Assertions.assertNotNull(httpClient);
    }
}
