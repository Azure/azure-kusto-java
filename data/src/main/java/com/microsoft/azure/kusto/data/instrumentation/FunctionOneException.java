package com.microsoft.azure.kusto.data.instrumentation;

/**
 * Supplier is a functional interface that can be used to execute data operations.
 * @param <T>
 * @param <V>
 */
public interface FunctionOneException<T, U, V extends Exception> {
    /**
     * Execute a data operation.
     * @param arg
     * @return T
     * @throws V
     */
    T apply(U arg) throws V;
}
