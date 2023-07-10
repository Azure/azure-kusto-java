package com.microsoft.azure.kusto.ingest.resources;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

import static com.microsoft.azure.kusto.ingest.ResourceAlgorithms.roundRobinNestedList;

public class RankedStorageAccountSet {
    private final Map<String, RankedStorageAccount> accounts;

    public RankedStorageAccountSet() {
        this.accounts = new HashMap<>();
    }

    public void addResultToAccount(String accountName, boolean success) {
        RankedStorageAccount account = accounts.get(accountName);
        if (account != null) {
            account.addResult(success);
        }
    }

    public void addAccount(String accountName) {
        if (!accounts.containsKey(accountName)) {
            accounts.put(accountName, new RankedStorageAccount(accountName));
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

    @NotNull
    public List<RankedStorageAccount> getRankedShuffledAccounts() {
        List<RankedStorageAccount> accounts = new ArrayList<>(this.accounts.values());
        // shuffle accounts
        Collections.shuffle(accounts);
        return accounts;
    }

}
