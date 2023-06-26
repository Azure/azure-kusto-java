package com.microsoft.azure.kusto.ingest.utils;

public interface ResourceWithSas<T> {
    String getEndpoint();

    String getAccountName();

    T getResource();
}
