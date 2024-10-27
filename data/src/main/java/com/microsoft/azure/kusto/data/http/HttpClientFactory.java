package com.microsoft.azure.kusto.data.http;

import com.microsoft.azure.kusto.data.HttpClientProperties;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A singleton factory of HTTP clients.
 */
public class HttpClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);

    /**
     * Creates a new Apache HTTP client.
     *
     * @param properties custom HTTP client properties
     * @return a new Apache HTTP client
     */
    public static CloseableHttpClient create(HttpClientProperties properties) {
        LOGGER.info("Creating new CloseableHttpClient client");
        if (properties == null) {
            properties = HttpClientProperties.builder().build();
        }

        final HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                .useSystemProperties()
                .setMaxConnTotal(properties.maxConnectionTotal())
                .setMaxConnPerRoute(properties.maxConnectionRoute())
                .evictExpiredConnections()
                .evictIdleConnections(properties.maxIdleTime(), TimeUnit.SECONDS)
                .disableRedirectHandling();

        if (properties.isKeepAlive()) {
            final ConnectionKeepAliveStrategy keepAliveStrategy = new CustomConnectionKeepAliveStrategy(properties.maxKeepAliveTime());
            httpClientBuilder.setKeepAliveStrategy(keepAliveStrategy);
        }

        if (properties.getPlanner() != null) {
            httpClientBuilder.setRoutePlanner(properties.getPlanner());
        }

        if (properties.getProxy() != null) {
            httpClientBuilder.setProxy(properties.getProxy());
        }

        if (properties.supportedProtocols() != null) {

            try {
                httpClientBuilder.setSSLSocketFactory(new SSLConnectionSocketFactory(SSLContext.getDefault(), properties.supportedProtocols(), null,
                        SSLConnectionSocketFactory.getDefaultHostnameVerifier()));
            } catch (NoSuchAlgorithmException e) {
                LOGGER.error("Failed to set supported protocols", e);
            }
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
