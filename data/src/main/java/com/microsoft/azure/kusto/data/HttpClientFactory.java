package com.microsoft.azure.kusto.data;

import org.apache.hc.client5.http.ConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.HeaderElement;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.message.BasicHeaderElementIterator;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public static CloseableHttpClient create(HttpClientProperties providedProperties) {
        LOGGER.info("Creating new CloseableHttpClient client");
        final HttpClientProperties properties = Optional.ofNullable(providedProperties)
                .orElse(HttpClientProperties.builder().build());
        final HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                .useSystemProperties()
                .setConnectionManager(
                        PoolingHttpClientConnectionManagerBuilder.create()
                                .setMaxConnTotal(properties.maxConnectionTotal())
                                .setMaxConnPerRoute(properties.maxConnectionRoute())
                                .build())
                .evictExpiredConnections()
                .evictIdleConnections(TimeValue.ofSeconds(properties.maxIdleTime()));

        if (properties.isKeepAlive()) {
            final ConnectionKeepAliveStrategy keepAliveStrategy = new CustomConnectionKeepAliveStrategy(properties.maxKeepAliveTime());
            httpClientBuilder.setKeepAliveStrategy(keepAliveStrategy);
        }

        if (properties.getProxy() != null) {
            httpClientBuilder.setProxy(properties.getProxy());
        }

        return httpClientBuilder.build();
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
