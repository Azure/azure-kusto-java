package com.microsoft.azure.kusto.ingest.resources;

public interface StorageAccountWeighingStrategy {
    void addResult(boolean success);

    double getSuccessRate();
}
