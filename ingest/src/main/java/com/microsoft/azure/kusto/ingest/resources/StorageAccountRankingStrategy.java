package com.microsoft.azure.kusto.ingest.resources;

public interface StorageAccountRankingStrategy {
    void addResult(boolean success);

    double getSuccessRate();
}
