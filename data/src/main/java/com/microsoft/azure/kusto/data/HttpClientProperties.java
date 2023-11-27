package com.microsoft.azure.kusto.data;

import org.apache.http.HttpHost;

/**
 * HTTP client properties.
 * TODO: move to http package on next major
 */
public class HttpClientProperties {
    private final Integer maxIdleTime;
    private final boolean keepAlive;
    private final Integer maxKeepAliveTime;
    private final Integer maxConnectionTotal;
    private final Integer maxConnectionRoute;
    private final HttpHost proxy;

    private HttpClientProperties(HttpClientPropertiesBuilder builder) {
        this.maxIdleTime = builder.maxIdleTime;
        this.keepAlive = builder.keepAlive;
        this.maxKeepAliveTime = builder.maxKeepAliveTime;
        this.maxConnectionTotal = builder.maxConnectionsTotal;
        this.maxConnectionRoute = builder.maxConnectionsPerRoute;
        this.proxy = builder.proxy;
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
     * Indicates whether or not a custom connection keep-alive time should be used. If set to {@code false}, the HTTP
     * client will use the default connection keep-alive strategy, which is to use only the server instructions
     * (if any) set in the {@code Keep-Alive} response header.
     * If set to {@code true}, the HTTP client will use a custom connection keep-alive strategy which uses the
     * server instructions set in the {@code Keep-Alive} response header; if the response doesn't contain a
     * {@code Keep-Alive} header, the client will use a default keep-alive period indicated by
     * {@linkplain #maxKeepAliveTime()}.
     *
     * @return whether a custom connection keep-alive strategy should be used
     *
     * @see #maxKeepAliveTime()
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Keep-Alive">Keep-Alive</a>
     */
    public boolean isKeepAlive() {
        return keepAlive;
    }

    /**
     * The time a connection can remain idle as part of the keep-alive strategy. This value is only used if
     * {@linkplain #isKeepAlive()} is set to {@code true}.
     * Defaults to 2 minutes.
     *
     * @return the maximum custom keep-alive time expressed in seconds
     */
    public Integer maxKeepAliveTime() {
        return maxKeepAliveTime;
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
     * The maximum number of connections the client may keep open at the same time per route.
     *
     * @return the maximum number of connections per route
     */
    public Integer maxConnectionRoute() {
        return maxConnectionRoute;
    }

    /**
     * The proxy to use when connecting to the remote server.
     *
     * @return the proxy
     */
    public HttpHost getProxy() {
        return proxy;
    }

    public static class HttpClientPropertiesBuilder {

        private Integer maxIdleTime = 120;
        private boolean keepAlive;
        private Integer maxKeepAliveTime = 120;
        private Integer maxConnectionsTotal = 40;
        private Integer maxConnectionsPerRoute = 40;
        private HttpHost proxy = null;

        private HttpClientPropertiesBuilder() {
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
         * {@code Keep-Alive} header, the client will use a default keep-alive period which is configurable via
         * {@linkplain #maxKeepAliveTime(Integer)}.
         *
         * @param keepAlive set to {@code false} to use a default keep-alive strategy or to {@code true} to use a
         *                  custom one
         * @return the builder instance
         *
         * @see #maxKeepAliveTime(Integer)
         * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Keep-Alive">Keep-Alive</a>
         */
        public HttpClientPropertiesBuilder keepAlive(boolean keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        /**
         * Sets the time a connection can remain idle as part of the keep-alive strategy. This value is only used if
         * {@linkplain #keepAlive(boolean)} is set to {@code true}.
         * Defaults to 120 seconds (2 minutes).
         *
         * @param maxKeepAliveTime the maximum time a connection may remain idle, expressed in seconds
         * @return the builder instance
         */
        public HttpClientPropertiesBuilder maxKeepAliveTime(Integer maxKeepAliveTime) {
            this.maxKeepAliveTime = maxKeepAliveTime;
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
         * Sets the maximum number of connections the client may keep open at the same time for the same route (endpoint).
         *
         * @param maxConnections the maximum number of connections per route
         * @return the builder instance
         */
        public HttpClientPropertiesBuilder maxConnectionsPerRoute(Integer maxConnections) {
            this.maxConnectionsPerRoute = maxConnections;
            return this;
        }

        /**
         * Sets a proxy server to use for the client.
         *
         * @param proxy the proxy server
         * @return the builder instance
         */
        public HttpClientPropertiesBuilder proxy(HttpHost proxy) {
            this.proxy = proxy;
            return this;
        }

        public HttpClientProperties build() {
            return new HttpClientProperties(this);
        }
    }

}
