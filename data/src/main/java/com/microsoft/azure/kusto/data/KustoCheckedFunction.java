package com.microsoft.azure.kusto.data;

@FunctionalInterface
public interface KustoCheckedFunction<T, R, E1 extends Throwable, E2 extends Throwable> {
    R apply(T t) throws E1, E2;
}
