package com.microsoft.azure.kusto.data.auth;

import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.azure.kusto.data.instrumentation.KustoSpan;
import com.microsoft.azure.kusto.data.instrumentation.KustoTracer;
import org.apache.http.client.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.font.EAttribute;

public abstract class TokenProviderBase {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final String clusterUrl;
    protected final HttpClient httpClient;

    public TokenProviderBase(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        this.clusterUrl = UriUtils.setPathForUri(clusterUrl, "");
        this.httpClient = httpClient;
    }
    public String acquireAccessToken() throws DataServiceException, DataClientException{

        // trace GetToken
        Map<String, String> attributes = new HashMap<>();
        attributes.put("authentication_method", getAuthMethodForTracing());
        KustoSpan kustoSpan = KustoTracer.startSpan("TokenProvider.acquireAccessToken", Context.NONE, ProcessKind.PROCESS, attributes);
        try (kustoSpan){
            return acquireAccessTokenInner();
        } catch (DataServiceException | DataClientException e){
            kustoSpan.addException(e);
            throw e;
        }
    }

    abstract String acquireAccessTokenInner() throws DataServiceException, DataClientException;
    protected abstract String getAuthMethodForTracing();
}
