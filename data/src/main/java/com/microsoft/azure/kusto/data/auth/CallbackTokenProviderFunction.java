package com.microsoft.azure.kusto.data.auth;

import org.apache.hc.client5.http.classic.HttpClient;

@FunctionalInterface
public interface CallbackTokenProviderFunction {
    String apply(HttpClient client) throws Exception;
}
