package com.microsoft.azure.kusto.ingest.resources;

public class RankedStorageAccount {
    private final String accountName;

    public RankedStorageAccount(String accountName) {
        this.accountName = accountName;
    }

    public void addResult(boolean success) {

    }

    public double getSuccessRate() {
        return 1;
    }

    public String getAccountName() {
        return accountName;
    }

}
