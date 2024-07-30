package com.microsoft.azure.kusto.data.http;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.util.Header;
import com.azure.core.util.HttpClientOptions;
import com.azure.core.util.TracingOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A static factory for HTTP clients.
 */
public class HttpClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);

    /**
     * Creates a new HTTP client.
     *
     * @param properties custom HTTP client properties
     * @return a new HTTP client
     */
    public static HttpClient create(HttpClientProperties properties) {
        LOGGER.info("Creating new HTTP Client");
        if (properties == null) {
            return HttpClient.createDefault();
        }

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

        TracingOptions tracingOptions = new TracingOptions();

        tracingOptions.setEnabled(true);
        // Todo: Is the per route connection maximum needed anymore?
        options.setTracingOptions(tracingOptions);
        return HttpClient.createDefault(options);
    }

}
