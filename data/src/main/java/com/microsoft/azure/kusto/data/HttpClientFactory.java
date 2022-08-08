package com.microsoft.azure.kusto.data;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
        final HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                .useSystemProperties()
                .setMaxConnTotal(properties.maxConnectionTotal())
                .setMaxConnPerRoute(properties.maxConnectionRoute())
                .evictExpiredConnections()
                .evictIdleConnections(properties.maxIdleTime(), TimeUnit.SECONDS);

        if (properties.isKeepAlive()) {
            final ConnectionKeepAliveStrategy keepAliveStrategy = new CustomConnectionKeepAliveStrategy(properties.maxKeepAliveTime());

            httpClientBuilder.setKeepAliveStrategy(keepAliveStrategy);
        }

        if (properties.getProxy() != null) {
            httpClientBuilder.setProxy(properties.getProxy());
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
        public long getKeepAliveDuration(HttpResponse httpResponse, HttpContext httpContext) {
            // honor 'keep-alive' header
            HeaderElementIterator it = new BasicHeaderElementIterator(httpResponse.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }
            // otherwise keep alive for default seconds
            return defaultKeepAlive * 1000L;
        }
    }

}
