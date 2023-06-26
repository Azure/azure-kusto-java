package com.microsoft.azure.kusto.ingest.utils;

public interface ResourceWithSas<T> {
    String getEndpointWithoutSas();

    String getAccountName();

    T getResource();
}
