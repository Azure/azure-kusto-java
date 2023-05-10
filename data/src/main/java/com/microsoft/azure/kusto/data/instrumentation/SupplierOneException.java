package com.microsoft.azure.kusto.data.instrumentation;

/**
 * Supplier is a functional interface that can be used to execute data operations.
 * @param <T>
 * @param <U>
 */
@FunctionalInterface
public interface SupplierOneException<T, U extends Exception> {
    /**
     * Execute a data operation.
     *
     * @return T
     * @throws U
     */
    T get() throws U;
}
