package com.microsoft.azure.kusto.ingest.resources;

public class SimpleStorageAccountWeighingStrategy implements StorageAccountWeighingStrategy {

    @Override
    public void addResult(boolean success) {
    }

    @Override
    public double getSuccessRate() {
        return 1;
    }
}
