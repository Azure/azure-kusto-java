package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.HttpClient;

@FunctionalInterface
public interface CallbackTokenProviderFunction {
    String apply(HttpClient client) throws Exception;
}
