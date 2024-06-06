package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.HttpClient;
import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public abstract class TokenProviderBase implements TraceableAttributes {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final String clusterUrl;
    protected final HttpClient httpClient;
    private final String authMethod;

    public TokenProviderBase(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        this.clusterUrl = UriUtils.setPathForUri(clusterUrl, "");
        this.httpClient = httpClient;
        this.authMethod = getClass().getSimpleName();
    }

    public Mono<String> acquireAccessToken() throws DataServiceException, DataClientException {
        return initialize().then(Mono.defer(() -> MonitoredActivity.wrap(this.acquireAccessTokenImpl(),
                getAuthMethod().concat(".acquireAccessToken"), getTracingAttributes())));
    }

    Mono<Void> initialize() {
        return Mono.empty();
    }

    protected abstract Mono<String> acquireAccessTokenImpl();

    protected String getAuthMethod() {
        return authMethod;
    }

    @Override
    public Map<String, String> getTracingAttributes() {
        return new HashMap<>();
    }
}
