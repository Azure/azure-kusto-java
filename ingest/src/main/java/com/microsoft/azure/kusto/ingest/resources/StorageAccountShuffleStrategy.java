package com.microsoft.azure.kusto.ingest.resources;

import java.util.List;
import java.util.Map;

public interface StorageAccountShuffleStrategy {
    <T> List<T> shuffleResources(List<RankedStorageAccount> accounts, Map<String, List<T>> resources);
}
