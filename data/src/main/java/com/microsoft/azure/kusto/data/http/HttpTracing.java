package com.microsoft.azure.kusto.data.http;

import com.microsoft.azure.kusto.data.ClientDetails;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import reactor.util.annotation.Nullable;

public class HttpTracing {
    private ClientRequestProperties properties;
    private String clientRequestIdPrefix;
    private String activityTypeSuffix;
    private ClientDetails clientDetails;

    private HttpTracing() {
    }

    @Nullable
    public ClientRequestProperties getProperties() {
        return properties;
    }

    public void setProperties(ClientRequestProperties properties) {
        this.properties = properties;
    }

    public String getClientRequestIdPrefix() {
        return clientRequestIdPrefix;
    }

    public void setClientRequestIdPrefix(String clientRequestIdPrefix) {
        this.clientRequestIdPrefix = clientRequestIdPrefix;
    }

    public String getActivityTypeSuffix() {
        return activityTypeSuffix;
    }

    public void setActivityTypeSuffix(String activityTypeSuffix) {
        this.activityTypeSuffix = activityTypeSuffix;
    }

    public ClientDetails getClientDetails() {
        return clientDetails;
    }

    public void setClientDetails(ClientDetails clientDetails) {
        this.clientDetails = clientDetails;
    }

    public static HttpTracingBuilder newBuilder() {
        return new HttpTracingBuilder();
    }

    public static class HttpTracingBuilder {

        private final HttpTracing tracing = new HttpTracing();

        public HttpTracingBuilder() {
        }

        public HttpTracingBuilder withClientDetails(ClientDetails clientDetails) {
            tracing.setClientDetails(clientDetails);
            return this;
        }

        public HttpTracingBuilder withProperties(ClientRequestProperties properties) {
            tracing.setProperties(properties);
            return this;
        }

        public HttpTracingBuilder withRequestPrefix(String requestPrefix) {
            tracing.setClientRequestIdPrefix(requestPrefix);
            return this;
        }

        public HttpTracingBuilder withActivitySuffix(String activitySuffix) {
            tracing.setActivityTypeSuffix(activitySuffix);
            return this;
        }

        public HttpTracing build() {
            return tracing;
        }

    }
}
