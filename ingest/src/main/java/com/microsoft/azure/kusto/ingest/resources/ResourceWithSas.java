package com.microsoft.azure.kusto.ingest.resources;

public interface ResourceWithSas<T> {
    String getEndpointWithoutSas();

    String getAccountName();

    T getResource();
}
