package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;

@FunctionalInterface
public interface KustoCheckedFunction<T, R> {
    R apply(T t) throws IngestionClientException, IngestionServiceException;
}
