package com.microsoft.azure.kusto.data.http;

import com.azure.core.http.HttpClient;
import com.azure.core.util.HttpClientOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

/**
 * A singleton factory of HTTP clients.
 */
public class HttpClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);

    /**
     * Creates a new Apache HTTP client.
     *
     * @param providedProperties custom HTTP client properties
     * @return a new Apache HTTP client
     */
    public static HttpClient create(HttpClientProperties providedProperties) {
        LOGGER.info("Creating new HTTP Client");
        final HttpClientProperties properties = Optional.ofNullable(providedProperties)
                .orElse(HttpClientProperties.builder().build());

        HttpClientOptions options = new HttpClientOptions();
        options.setMaximumConnectionPoolSize(properties.maxConnectionTotal());
        options.setConnectionIdleTimeout(Duration.ofSeconds(properties.maxIdleTime()));

        if (properties.getProxy() != null) {
            // HttpHost host = properties.getProxy();
            // Todo set proxy options
            options.setProxyOptions(null);
        }

        // Is the per route connection maximum needed?
        // Todo: missing keepalive and per-route connections, is keepalive handled by default in the azure clients?

        return HttpClient.createDefault(options);
    }

}
