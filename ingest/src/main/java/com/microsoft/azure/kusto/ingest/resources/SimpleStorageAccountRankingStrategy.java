package com.microsoft.azure.kusto.ingest.resources;

public class SimpleStorageAccountRankingStrategy implements StorageAccountRankingStrategy {

    @Override
    public void addResult(boolean success) {
    }

    @Override
    public double getSuccessRate() {
        return 1;
    }
}
