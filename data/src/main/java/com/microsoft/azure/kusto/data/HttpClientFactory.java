package com.microsoft.azure.kusto.data;

import org.apache.hc.client5.http.ConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HeaderElement;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.message.BasicHeaderElementIterator;
import org.apache.hc.core5.http.protocol.HttpContext;

import org.apache.hc.core5.util.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * A singleton factory of HTTP clients.
 */
class HttpClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);

    private HttpClientFactory() {
    }

    private static class HttpClientFactorySingleton {

        private static final HttpClientFactory instance = new HttpClientFactory();
    }

    /**
     * Returns the factory instance.
     *
     * @return the shared {@linkplain HttpClientFactory} instance
     */
    static HttpClientFactory getInstance() {
        return HttpClientFactorySingleton.instance;
    }

    /**
     * Creates a new Apache HTTP client.
     *
     * @param providedProperties custom HTTP client properties
     * @return a new Apache HTTP client
     */
    CloseableHttpClient create(HttpClientProperties providedProperties) {
        final HttpClientProperties properties = Optional.ofNullable(providedProperties)
                .orElse(HttpClientProperties.builder().build());
        //TODO: Determine if maxConnections/maxConnectionsPerRoute still exists in V5 of Apache HTTP Components and if configurable
        final HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                .useSystemProperties()
                .evictExpiredConnections()
                .evictIdleConnections(TimeValue.ofSeconds(properties.maxIdleTime()));

        if (properties.isKeepAlive()) {
            final ConnectionKeepAliveStrategy keepAliveStrategy = new CustomConnectionKeepAliveStrategy(properties.maxKeepAliveTime());
            httpClientBuilder.setKeepAliveStrategy(keepAliveStrategy);
        }

        final CloseableHttpClient httpClient = httpClientBuilder.build();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeClient(httpClient)));
        return httpClient;
    }

    private static void closeClient(CloseableHttpClient client) {
        try {
            LOGGER.info("Closing HTTP client");
            client.close();
        } catch (IOException e) {
            LOGGER.warn("Couldn't close HTTP client.");
        }
    }

    /**
     * A custom connection keep-alive strategy that uses the server instructions set in the {@code Keep-Alive}
     * response header; if the response doesn't contain a {@code Keep-Alive} header, the client will use a configurable
     * keep-alive period.
     */
    static class CustomConnectionKeepAliveStrategy implements ConnectionKeepAliveStrategy {

        private final int defaultKeepAlive;

        /**
         * The default keep-alive time.
         *
         * @param defaultKeepAlive the keep-alive time expressed in seconds
         */
        CustomConnectionKeepAliveStrategy(int defaultKeepAlive) {
            this.defaultKeepAlive = defaultKeepAlive;
        }

        @Override
        public TimeValue getKeepAliveDuration(HttpResponse httpResponse, HttpContext httpContext) {

            // honor 'keep-alive' header
            BasicHeaderElementIterator it = new BasicHeaderElementIterator(httpResponse.headerIterator("keep-alive"));
            while (it.hasNext()) {
                HeaderElement he = it.next();
                if (he.getValue() != null && he.getName().equalsIgnoreCase("timeout")) {
                    return TimeValue.ofMilliseconds(Long.parseLong(he.getValue()) * 1000L);
                }
            }
            // otherwise keep alive for default seconds
            return TimeValue.ofMilliseconds(defaultKeepAlive * 1000L);
        }
    }

}
