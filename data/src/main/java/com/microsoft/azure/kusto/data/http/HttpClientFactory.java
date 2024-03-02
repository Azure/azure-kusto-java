package com.microsoft.azure.kusto.data.http;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.netty.NettyAsyncHttpClientProvider;
import com.azure.core.util.Header;
import com.azure.core.util.HttpClientOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A singleton factory of HTTP clients.
 */
public class HttpClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);

    /**
     * Creates a new HTTP client.
     *
     * @param providedProperties custom HTTP client properties
     * @return a new HTTP client
     */
    public static HttpClient create(HttpClientProperties providedProperties) {
        LOGGER.info("Creating new HTTP Client");
        final HttpClientProperties properties = Optional.ofNullable(providedProperties)
                .orElse(HttpClientProperties.builder().build());

        // Docs: https://learn.microsoft.com/en-us/java/api/com.azure.core.util.httpclientoptions?view=azure-java-stable
        HttpClientOptions options = new HttpClientOptions();
        options.setMaximumConnectionPoolSize(properties.maxConnectionTotal());
        options.setConnectionIdleTimeout(Duration.ofSeconds(properties.maxIdleTime()));

        // properties.timeout() value could be null, Azure Core JavaDocs indicate this is OK.
        options.setResponseTimeout(properties.timeout());

        // If null (as it is in the builder) the first discovered HttpClientProvider class is loaded.
        // Netty is included by default in azure-core but can be excluded in the pom by excluding azure-core-http-netty.
        options.setHttpClientProvider(properties.provider());

        // Set Keep-Alive headers if they were requested.
        // NOTE: Servers are not obligated to honor client requested Keep-Alive values
        if (properties.isKeepAlive()) {
            Header keepAlive = new Header(HttpHeaderName.CONNECTION.getCaseSensitiveName(), "Keep-Alive");
            // Keep-Alive is Non-standard from the client so core does not have an enum for it
            Header keepAliveTimeout = new Header("Keep-Alive", "timeout=" + properties.maxKeepAliveTime());

            List<Header> headers = new ArrayList<>();
            headers.add(keepAlive);
            headers.add(keepAliveTimeout);

            options.setHeaders(headers);
        }

        if (properties.getProxy() != null) {
            options.setProxyOptions(properties.getProxy());
        }

        // Todo: Is the per route connection maximum needed anymore?

        return new NettyAsyncHttpClientProvider().createInstance(options);
    }

}
