package com.microsoft.azure.kusto.data;

import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.microsoft.azure.kusto.data.instrumentation.Tracer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TracingTest {

    @Test
    public void testStartSpanWithTracer() {
        com.azure.core.util.tracing.Tracer mockTracer = mock(com.azure.core.util.tracing.Tracer.class);
        Context mockContext = mock(Context.class);
        ProcessKind mockKind = ProcessKind.PROCESS;
        Map<String, String> mockAttributes = new HashMap<>();
        Tracer.initializeTracer(mockTracer);
        Tracer.Span span = Tracer.startSpan("testSpan", mockContext, mockKind, mockAttributes);
        assertNotNull(span);
    }

    @Test
    public void testStartSpanWithoutTracer() {
        Context mockContext = mock(Context.class);
        ProcessKind mockKind = ProcessKind.PROCESS;
        Map<String, String> mockAttributes = new HashMap<>();
        Tracer.initializeTracer(null);
        Tracer.Span span = Tracer.startSpan("testSpan", mockContext, mockKind, mockAttributes);
        assertNotNull(span);
    }

}
