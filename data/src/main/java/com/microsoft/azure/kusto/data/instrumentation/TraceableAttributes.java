package com.microsoft.azure.kusto.data.instrumentation;

import java.util.Map;

public interface TraceableAttributes {
    Map<String, String> addTraceAttributes(Map<String, String> attributes);
}
