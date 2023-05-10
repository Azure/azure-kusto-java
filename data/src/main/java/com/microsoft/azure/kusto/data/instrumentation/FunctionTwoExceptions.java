package com.microsoft.azure.kusto.data.instrumentation;

/**
 * FunctionTwoExceptions is a functional interface that can be used to execute data operations.
 * @param <T>
 * @param <V1>
 * @param <V2>
 */
public interface FunctionTwoExceptions<T, U, V1 extends Exception, V2 extends Exception> {
    /**
     * Execute a data operation.
     * @param arg
     * @return T
     * @throws V1
     * @throws V2
     */
    T apply(U arg) throws V1, V2;
}
