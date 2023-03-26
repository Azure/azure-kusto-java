package com.microsoft.azure.kusto.data.instrumentation;

import com.azure.core.util.Context;

public class KustoSpan implements AutoCloseable{
    private final Context span;
    private Throwable throwable;
    public KustoSpan(Context span) {
        this.span = span;
    }

    @Override
    public void close() {
        KustoTracer.endSpan(throwable, span, null);
    }

    public void addException(Exception e) {
        throwable = e;
    }
}
