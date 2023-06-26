package com.microsoft.azure.kusto.ingest.resources;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleStorageAccountShuffleStrategy implements StorageAccountShuffleStrategy {
    @Override
    public <T> List<T> shuffleResources(List<RankedStorageAccount> accounts, Map<String, List<T>> resources) {
        // shuffle accounts
        Collections.shuffle(accounts);

        // Get valid resources for each account
        List<List<T>> validResources = accounts.stream().map(x -> resources.get(x.getAccountName())).filter(r -> r != null && !r.isEmpty()).collect(Collectors.toList());

        int longestResourceList = validResources.stream().mapToInt(List::size).max().orElse(0);

        // naive round robin, get the nth element from each list, if it exists
        return IntStream.range(0, longestResourceList).boxed().flatMap(i -> validResources.stream().map(r -> r.size() > i ? r.get(i) : null).filter(Objects::nonNull)).collect(Collectors.toList());
    }
}
