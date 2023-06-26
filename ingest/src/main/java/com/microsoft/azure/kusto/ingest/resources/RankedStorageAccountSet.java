package com.microsoft.azure.kusto.ingest.resources;

import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RankedStorageAccountSet {
    private final StorageAccountShuffleStrategy storageAccountShuffleStrategy;
    private final StorageAccountWeighingStrategy strategy;
    private final Map<String, RankedStorageAccount> accounts;

    public RankedStorageAccountSet(StorageAccountShuffleStrategy shuffleStrategy, StorageAccountWeighingStrategy weighingStrategy) {
        this.storageAccountShuffleStrategy = shuffleStrategy;
        this.strategy = weighingStrategy;
        this.accounts = new HashMap<>();
    }

    public static RankedStorageAccountSet Simple() {
        return new RankedStorageAccountSet(new SimpleStorageAccountShuffleStrategy(), new SimpleStorageAccountWeighingStrategy());
    }

    public void addResultToAccount(String accountName, boolean success) {
        RankedStorageAccount account = accounts.get(accountName);
        if (account != null) {
            account.addResult(success);
        }
    }

    public void addAccount(String accountName) {
        if (!accounts.containsKey(accountName)) {
            accounts.put(accountName, new RankedStorageAccount(accountName, strategy));
        }
    }

    public void addAccount(RankedStorageAccount account) {
        if (!accounts.containsKey(account.getAccountName())) {
            accounts.put(account.getAccountName(), account);
        }
    }

    @Nullable
    public RankedStorageAccount getAccount(String accountName) {
        return accounts.get(accountName);
    }

    public <T> List<T> getShuffledResources(Map<String, List<T>> resourceSet) {
        List<RankedStorageAccount> accounts = new ArrayList<>(this.accounts.values());
        return storageAccountShuffleStrategy.shuffleResources(accounts, resourceSet);
    }

    public StorageAccountShuffleStrategy getShuffleStrategy() {
        return storageAccountShuffleStrategy;
    }

    public StorageAccountWeighingStrategy getWeighingStrategy() {
        return strategy;
    }

    public Map<String, RankedStorageAccount> getAccounts() {
        return accounts;
    }
}
