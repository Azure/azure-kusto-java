package com.microsoft.azure.kusto.data.auth;

import org.apache.http.client.HttpClient;

@FunctionalInterface
public interface CallbackTokenProviderFunction {
    String apply(HttpClient client) throws Exception;
}
