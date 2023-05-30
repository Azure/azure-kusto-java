package com.microsoft.azure.kusto.data.instrumentation;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

public interface TraceableAttributes {
    Map<String, String> getTracingAttributes(@NotNull Map<String, String> attributes);
}
