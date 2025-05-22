package com.microsoft.azure.kusto.data.http;

import com.azure.core.http.HttpClientProvider;
import com.azure.core.http.ProxyOptions;

/**
 * HTTP client properties.
 */
public class HttpClientProperties {
    private final Integer maxIdleTime;
    private final boolean keepAlive;
    private final Integer maxConnectionTotal;
    private final Class<? extends HttpClientProvider> provider;
    private final ProxyOptions proxy;
    private final Integer readTimeout;

    private HttpClientProperties(HttpClientPropertiesBuilder builder) {
        this.maxIdleTime = builder.maxIdleTime;
        this.keepAlive = builder.keepAlive;
        this.maxConnectionTotal = builder.maxConnectionsTotal;
        this.provider = builder.provider;
        this.proxy = builder.proxy;
        this.readTimeout = builder.readTimeout;
    }

    /**
     * Instantiates a new builder.
     *
     * @return a new {@linkplain HttpClientPropertiesBuilder}
     */
    public static HttpClientPropertiesBuilder builder() {
        return new HttpClientPropertiesBuilder();
    }

    /**
     * The maximum time persistent connections can stay idle while kept alive in the connection pool. Connections whose
     * inactivity period exceeds this value will get closed and evicted from the pool.
     *
     * @return the maximum idle time expressed in seconds
     */
    public Integer maxIdleTime() {
        return maxIdleTime;
    }

    /**
     * The amount of time between each response data read from the network before timing out.
     * Defaults to 1 hour.
     * @return read timeout in seconds
     * @see <a href="https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/core/azure-core/README.md#http-timeouts">azure-core timouts</a>
     */
    public Integer readTimeout() {
        return readTimeout;
    }

    /**
     * Indicates whether a custom connection keep-alive time should be used. If set to {@code false}, the HTTP
     * client will use the default connection keep-alive strategy, which is to use only the server instructions
     * (if any) set in the {@code Keep-Alive} response header.
     * If set to {@code true}, the HTTP client will use a custom connection keep-alive strategy which uses the
     * server instructions set in the {@code Keep-Alive} response header; if the response doesn't contain a
     *
     * @return whether a custom connection keep-alive strategy should be used
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Keep-Alive">Keep-Alive</a>
     */
    public boolean isKeepAlive() {
        return keepAlive;
    }

    /**
     * The maximum number of connections the client may keep open at the same time across all routes.
     *
     * @return the maximum number of connections
     */
    public Integer maxConnectionTotal() {
        return maxConnectionTotal;
    }

    /**
     * Gets the HTTP Client Provider used by Azure Core when constructing HTTP Client instances.
     *
     * @return the provider
     */
    public Class<? extends HttpClientProvider> provider() {
        return provider;
    }

    /**
     * The proxy to use when connecting to the remote server.
     *
     * @return the proxy
     */
    public ProxyOptions getProxy() {
        return proxy;
    }

    public static class HttpClientPropertiesBuilder {

        private Integer maxIdleTime = 120;
        private boolean keepAlive;
        private Integer readTimeout = 60 * 60;
        private Integer maxConnectionsTotal = 40;
        private Class<? extends HttpClientProvider> provider = null;
        private ProxyOptions proxy = null;

        public HttpClientPropertiesBuilder() {
        }

        /**
         * Set the maximum time persistent connections can stay idle while kept alive in the connection pool.
         * Connections whose inactivity period exceeds this value will get closed and evicted from the pool.
         * Defaults to 120 seconds (2 minutes).
         *
         * @param maxIdleTime the maximum idle time expressed in seconds
         * @return the builder instance
         */
        public HttpClientPropertiesBuilder maxIdleTime(Integer maxIdleTime) {
            this.maxIdleTime = maxIdleTime;
            return this;
        }

        /**
         * Set whether or not a custom connection keep-alive time should be used. If set to {@code false}, the HTTP
         * client will use the default connection keep-alive strategy, which is to use only the server instructions
         * (if any) set in the {@code Keep-Alive} response header.
         * If set to {@code true}, the HTTP client will use a custom connection keep-alive strategy which uses the
         * server instructions set in the {@code Keep-Alive} response header; if the response doesn't contain a
         *
         * @param keepAlive set to {@code false} to use a default keep-alive strategy or to {@code true} to use a
         *                  custom one
         * @return the builder instance
         * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Keep-Alive">Keep-Alive</a>
         */
        public HttpClientPropertiesBuilder keepAlive(boolean keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        /**
         * Sets the amount of time between each response data read from the network before timing out.
         * Defaults to 1 hour.
         * @param readTimeout the maximum custom keep-alive time expressed in seconds
         * @return the builder instance
         * @see <a href="https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/core/azure-core/README.md#http-timeouts">azure-core timouts</a>
         */
        public HttpClientPropertiesBuilder readTimeout(Integer readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        /**
         * Sets the total maximum number of connections the client may keep open at the same time.
         *
         * @param maxConnectionsTotal the total maximum number of connections
         * @return the builder instance
         */
        public HttpClientPropertiesBuilder maxConnectionsTotal(Integer maxConnectionsTotal) {
            this.maxConnectionsTotal = maxConnectionsTotal;
            return this;
        }

        /**
         * Sets the HTTP Client Provider used by Azure Core when constructing HTTP Client instances.
         *
         * @param provider the requested HTTP Client provider class
         * @return the builder instance
         */
        public HttpClientPropertiesBuilder provider(Class<? extends HttpClientProvider> provider) {
            this.provider = provider;
            return this;
        }

        /**
         * Sets a proxy server to use for the client.
         *
         * @param proxy the proxy server
         * @return the builder instance
         */
        public HttpClientPropertiesBuilder proxy(ProxyOptions proxy) {
            this.proxy = proxy;
            return this;
        }

        public HttpClientProperties build() {
            return new HttpClientProperties(this);
        }
    }
}
