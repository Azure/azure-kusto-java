package com.microsoft.azure.kusto.data;

import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.azure.core.util.tracing.Tracer;
import com.microsoft.azure.kusto.data.instrumentation.DistributedTracing;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TracingTest {

    @Test
    public void testStartSpanWithTracer() {
        Tracer mockTracer = mock(Tracer.class);
        Context mockContext = mock(Context.class);
        ProcessKind mockKind = ProcessKind.PROCESS;
        Map<String, String> mockAttributes = new HashMap<>();
        DistributedTracing.initializeTracer(mockTracer);
        DistributedTracing.Span span = DistributedTracing.startSpan("testSpan", mockContext, mockKind, mockAttributes);
        assertNotNull(span);
    }

    @Test
    public void testStartSpanWithoutTracer() {
        Context mockContext = mock(Context.class);
        ProcessKind mockKind = ProcessKind.PROCESS;
        Map<String, String> mockAttributes = new HashMap<>();
        DistributedTracing.initializeTracer(null);
        DistributedTracing.Span span = DistributedTracing.startSpan("testSpan", mockContext, mockKind, mockAttributes);
        assertNotNull(span);
    }
}
