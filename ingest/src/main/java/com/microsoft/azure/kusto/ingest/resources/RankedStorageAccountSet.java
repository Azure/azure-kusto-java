package com.microsoft.azure.kusto.ingest.resources;

import com.microsoft.azure.kusto.ingest.utils.DefaultRandomProvider;
import com.microsoft.azure.kusto.ingest.utils.RandomProvider;
import com.microsoft.azure.kusto.ingest.utils.SystemTimeProvider;
import com.microsoft.azure.kusto.ingest.utils.TimeProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class RankedStorageAccountSet {
    private static final int DEFAULT_BUCKET_COUNT = 6;
    private static final int DEFAULT_BUCKET_DURATION_IN_SEC = 10;
    private static final int[] DEFAULT_TIERS = new int[]{90, 70, 30, 0};

    public static final TimeProvider DEFAULT_TIME_PROVIDER = new SystemTimeProvider();
    public static final RandomProvider DEFAULT_RANDOM_PROVIDER = new DefaultRandomProvider();
    private final Map<String, RankedStorageAccount> accounts;
    private final int bucketCount;
    private final int bucketDurationInSec;
    private final int[] tiers;
    private final TimeProvider timeProvider;
    private final RandomProvider randomProvider;

    public RankedStorageAccountSet(int bucketCount, int bucketDurationInSec, int[] tiers, TimeProvider timeProvider, RandomProvider randomProvider) {
        this.bucketCount = bucketCount;
        this.bucketDurationInSec = bucketDurationInSec;
        this.tiers = tiers;
        this.timeProvider = timeProvider;
        this.randomProvider = randomProvider;
        this.accounts = new HashMap<>();
    }

    public RankedStorageAccountSet() {
        this(DEFAULT_BUCKET_COUNT, DEFAULT_BUCKET_DURATION_IN_SEC, DEFAULT_TIERS, DEFAULT_TIME_PROVIDER, DEFAULT_RANDOM_PROVIDER);
    }

    public void addResultToAccount(String accountName, boolean success) {
        RankedStorageAccount account = accounts.get(accountName);
        if (account != null) {
            account.addResult(success);
        } else {
            throw new IllegalArgumentException("Account " + accountName + " does not exist");
        }
    }

    public void addAccount(String accountName) {
        if (!accounts.containsKey(accountName)) {
            accounts.put(accountName, new RankedStorageAccount(accountName, bucketCount, bucketDurationInSec, timeProvider));
        } else {
            throw new IllegalArgumentException("Account " + accountName + " already exists");
        }
    }

    public void addAccount(RankedStorageAccount account) {
        if (!accounts.containsKey(account.getAccountName())) {
            accounts.put(account.getAccountName(), account);
        } else {
            throw new IllegalArgumentException("Account " + account.getAccountName() + " already exists");
        }
    }

    @Nullable
    public RankedStorageAccount getAccount(String accountName) {
        return accounts.get(accountName);
    }

    @NotNull
    public List<RankedStorageAccount> getRankedShuffledAccounts() {
        List<List<RankedStorageAccount>> tiersList = new ArrayList<>();

        for (int i = 0; i < tiers.length; i++) {
            tiersList.add(new ArrayList<>());
        }

        for (RankedStorageAccount account : this.accounts.values()) {
            double rankPercentage = account.getRank() * 100.0;
            for (int i = 0; i < tiers.length; i++) {
                if (rankPercentage >= tiers[i]) {
                    tiersList.get(i).add(account);
                    break;
                }
            }
        }

        for (List<RankedStorageAccount> tier : tiersList) {
            randomProvider.shuffle(tier);
        }

        // flatten tiers
        return tiersList.stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

}
