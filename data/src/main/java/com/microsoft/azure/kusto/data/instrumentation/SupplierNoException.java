package com.microsoft.azure.kusto.data.instrumentation;

/**
 * Supplier is a functional interface that can be used to execute data operations.
 * @param <T>
 */
@FunctionalInterface
public interface SupplierNoException<T> {

    /**
     * Execute a data operation.
     * @return T
     */
    T get();

}
