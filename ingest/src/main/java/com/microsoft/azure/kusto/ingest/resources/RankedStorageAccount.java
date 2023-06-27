package com.microsoft.azure.kusto.ingest.resources;

public class RankedStorageAccount {
    private final String accountName;
    private final StorageAccountRankingStrategy strategy;

    public RankedStorageAccount(String accountName, StorageAccountRankingStrategy strategy) {
        this.accountName = accountName;
        this.strategy = strategy;
    }

    public void addResult(boolean success) {
        strategy.addResult(success);
    }

    public double getSuccessRate() {
        return strategy.getSuccessRate();
    }

    public String getAccountName() {
        return accountName;
    }

    public StorageAccountRankingStrategy getStrategy() {
        return strategy;
    }
}
