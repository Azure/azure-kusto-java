package com.microsoft.azure.kusto.data.http;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientProvider;
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

        // properties.timeout() value could be null, Azure Core JavaDocs indicate this is OK.
        options.setResponseTimeout(properties.timeout());

        // If changed to OKHttp - change in ResourceManager - as well
        options.setHttpClientProvider(properties.provider());

        if (properties.getProxy() != null) {
            options.setProxyOptions(properties.getProxy());
        }

        // Is the per route connection maximum needed anymore?

        // Todo: missing keepalive and per-route connections.
        // NOTE: Keep-Alive is provided by 2 headers that can be placed on any HTTP request or response.
        // It looks like one of the headers, Connection: Keep-Alive, is already configured when setting up tracing
        // See HttpRequestBuilder withTracing() method for details.
        // We could easily add the second header, Keep-Alive: timeout=numSeconds, max=numConnections
        // Will add this to builder if wanted

        // NOTE: Keep-Alive is usually dictated by the server. The client makes requests but the server is under
        // no obligation to honor the Keep-Alive headers passed from the client, and will often replace them.
        // See: https://stackoverflow.com/questions/19155201/http-keep-alive-timeout

        // Keep-Alive is additionally prohibited in HTTP2 and HTTP3 requests
        // This is because in HTTP2 the default option is to hold open a single TCP channel for all requests
        // And in HTTP3, TCP is no longer used as the underlying communication channel. HTTP3 is instead UDP based.
        // See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Keep-Alive

        return new NettyAsyncHttpClientProvider().createInstance(options);
    }

}
