package com.microsoft.azure.kusto.ingest.utils;

import com.microsoft.azure.kusto.ingest.IngestionProperties;

@FunctionalInterface
public interface ShouldUseQueueingPredicate {
    Boolean apply(long size, long rawDataSize, boolean compressed, IngestionProperties.DataFormat format);
}
