package com.microsoft.azure.kusto.ingest.utils;

@FunctionalInterface
public interface ConsumerWithException<T> {
    void accept(T t) throws Exception;
}
