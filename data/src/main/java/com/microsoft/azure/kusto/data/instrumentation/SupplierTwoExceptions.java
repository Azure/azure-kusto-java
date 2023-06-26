package com.microsoft.azure.kusto.data.instrumentation;

/**
 * Supplier is a functional interface that can be used to execute data operations.
 * @param <T>
 * @param <U1>
 * @param <U2>
 */
@FunctionalInterface
public interface SupplierTwoExceptions<T, U1 extends Exception, U2 extends Exception> {
    /**
     * Execute a data operation.
     * @return T
     * @throws U1
     * @throws U2
     */
    T get() throws U1, U2;
}
