package com.microsoft.azure.kusto.ingest.utils;

public interface TimeProvider {
    long currentTimeMillis();
}
