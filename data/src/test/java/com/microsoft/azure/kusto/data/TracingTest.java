package com.microsoft.azure.kusto.data;

import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.microsoft.azure.kusto.data.instrumentation.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TracingTest {
    private com.azure.core.util.tracing.Tracer mockTracer;
    private Context mockContext;
    private ProcessKind mockKind;
    private Map<String, String> mockAttributes;

    @BeforeEach
    public void setUp() {
        mockTracer = mock(com.azure.core.util.tracing.Tracer.class);
        mockContext = mock(Context.class);
        mockKind = ProcessKind.PROCESS;
        mockAttributes = new HashMap<>();
    }

    @AfterEach
    public void tearDown() {
        try {
            Field field = Tracer.class.getDeclaredField("initialized");
            field.setAccessible(true);
            field.set(null, false);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStartSpanWithTracer() {
        Tracer.initializeTracer(mockTracer);
        Tracer.Span span = Tracer.startSpan("testSpan");
        assertNotNull(span);
    }

    @Test
    public void testStartSpanWithoutTracer() {
        Tracer.initializeTracer(null);
        Tracer.Span span = Tracer.startSpan("testSpan", mockContext, mockKind, mockAttributes);
        assertNotNull(span);
    }

    @Test
    public void testStartSpanCalled() {
        Tracer.initializeTracer(mockTracer);
        Tracer.Span span = Tracer.startSpan("testSpan", mockContext, mockKind, mockAttributes);
        verify(mockTracer, times(1)).start("testSpan", mockContext, mockKind);
    }

    @Test
    public void testStartSpanNotCalled() {
        Tracer.initializeTracer(null);
        Tracer.Span span = Tracer.startSpan("testSpan", mockContext, mockKind, mockAttributes);
        verify(mockTracer, times(0)).start("testSpan", mockContext, mockKind);
    }

    @Test
    public void testSpanCloseWithTracer() {
        Tracer.initializeTracer(mockTracer);
        when(mockTracer.start(eq("testSpan"), eq(mockContext), eq(mockKind))).thenReturn(mockContext);
        Tracer.Span span = Tracer.startSpan("testSpan", mockContext, mockKind, mockAttributes);
        span.close();
        verify(mockTracer, times(1)).end(eq("success"), isNull(), eq(mockContext));
    }

    @Test
    public void testSpanCloseWithTracerAndException() {
        Tracer.initializeTracer(mockTracer);
        when(mockTracer.start(eq("testSpan"), eq(mockContext), eq(mockKind))).thenReturn(mockContext);
        Tracer.Span span = Tracer.startSpan("testSpan", mockContext, mockKind, mockAttributes);
        Exception e = new Exception("testException");
        span.addException(e);
        span.close();
        verify(mockTracer, times(1)).end(eq("testException"), eq(e), eq(mockContext));
    }

    @Test
    public void testSetAttributes() {
        Tracer.initializeTracer(mockTracer);
        when(mockTracer.start(eq("testSpan"), eq(mockContext), eq(mockKind))).thenReturn(mockContext);
        mockAttributes.put("testKey", "testValue");
        Tracer.Span span = Tracer.startSpan("testSpan", mockContext, mockKind, mockAttributes);
        span.setAttributes(mockAttributes);
        for (Map.Entry<String, String> entry : mockAttributes.entrySet()) {
            verify(mockTracer, times(2)).setAttribute(eq(entry.getKey()), eq(entry.getValue()), eq(mockContext));
        }
        span.setAttributes(null);
        for (Map.Entry<String, String> entry : mockAttributes.entrySet()) {
            verify(mockTracer, times(2)).setAttribute(eq(entry.getKey()), eq(entry.getValue()), eq(mockContext));
        }
        mockAttributes.put("testKey", "testValue2");
        span.setAttributes(mockAttributes);
        for (Map.Entry<String, String> entry : mockAttributes.entrySet()) {
            verify(mockTracer, times(1)).setAttribute(eq(entry.getKey()), eq(entry.getValue()), eq(mockContext));
        }
    }

    @Test
    public void testNoAttributes() {
        Tracer.initializeTracer(mockTracer);
        when(mockTracer.start(eq("testSpan"), eq(mockContext), eq(mockKind))).thenReturn(mockContext);
        Tracer.Span span = Tracer.startSpan("testSpan", mockContext, mockKind, null);
        span.setAttributes(null);
        verify(mockTracer, times(0)).setAttribute(anyString(), anyString(), any());
    }

    @Test
    public void testInvokeRunnable() {
        Tracer.initializeTracer(mockTracer);
        when(mockTracer.start(eq("testSpan"), eq(Context.NONE), eq(ProcessKind.PROCESS))).thenReturn(mockContext);
        Runnable mockRunnable = mock(Runnable.class);

        String spanName = "testSpan";
        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");

        MonitoredActivity.invoke(mockRunnable, spanName, attributes);

        verify(mockTracer, times(1)).start(eq(spanName), eq(Context.NONE), eq(ProcessKind.PROCESS));
        verify(mockRunnable, times(1)).run();
        verify(mockTracer).end(eq("success"), isNull(), eq(mockContext));
    }

    @Test
    public void testInvokeSupplierOneException() throws Exception {
        Tracer.initializeTracer(mockTracer);
        when(mockTracer.start(eq("testSpan"), eq(Context.NONE), eq(mockKind))).thenReturn(mockContext);
        SupplierOneException mockSupplierOneException = mock(SupplierOneException.class);
        Exception e = new Exception("testException");
        when(mockSupplierOneException.get()).thenThrow(e);

        String spanName = "testSpan";
        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");

        try {
            MonitoredActivity.invoke(mockSupplierOneException, spanName, attributes);
        } catch (Exception ex) {
            assertEquals(e, ex);
        }

        verify(mockTracer, times(1)).start(eq(spanName), eq(Context.NONE), eq(mockKind));
        verify(mockSupplierOneException, times(1)).get();
        verify(mockTracer).end(eq("testException"), eq(e), eq(mockContext));

        reset(mockSupplierOneException);
        when(mockSupplierOneException.get()).thenReturn("testResult");
        MonitoredActivity.invoke(mockSupplierOneException, spanName, attributes);

        verify(mockTracer, times(2)).start(eq(spanName), eq(Context.NONE), eq(mockKind));
        verify(mockSupplierOneException, times(1)).get();
        verify(mockTracer).end(eq("success"), isNull(), eq(mockContext));
    }

    @Test
    public void testInvokeSupplierTwoExceptions() throws Exception {
        Tracer.initializeTracer(mockTracer);
        when(mockTracer.start(eq("testSpan"), eq(Context.NONE), eq(mockKind))).thenReturn(mockContext);
        SupplierTwoExceptions mockSupplierTwoExceptions = mock(SupplierTwoExceptions.class);
        Exception e = new Exception("testException");
        when(mockSupplierTwoExceptions.get()).thenThrow(e);

        String spanName = "testSpan";
        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");

        try {
            MonitoredActivity.invoke(mockSupplierTwoExceptions, spanName, attributes);
        } catch (Exception ex) {
            assertEquals(e, ex);
        }

        verify(mockTracer, times(1)).start(eq(spanName), eq(Context.NONE), eq(mockKind));
        verify(mockSupplierTwoExceptions, times(1)).get();
        verify(mockTracer).end(eq("testException"), eq(e), eq(mockContext));

        reset(mockSupplierTwoExceptions);
        when(mockSupplierTwoExceptions.get()).thenReturn("testResult");
        MonitoredActivity.invoke(mockSupplierTwoExceptions, spanName, attributes);

        verify(mockTracer, times(2)).start(eq(spanName), eq(Context.NONE), eq(mockKind));
        verify(mockSupplierTwoExceptions, times(1)).get();
        verify(mockTracer).end(eq("success"), isNull(), eq(mockContext));
    }

    @Test
    public void testInvokeFunctionOneException() throws Exception {
        Tracer.initializeTracer(mockTracer);
        when(mockTracer.start(eq("testSpan"), eq(Context.NONE), eq(mockKind))).thenReturn(mockContext);
        FunctionOneException mockFunctionOneException = mock(FunctionOneException.class);
        Exception e = new Exception("testException");
        when(mockFunctionOneException.apply(any())).thenThrow(e);

        String spanName = "testSpan";
        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        try {
            MonitoredActivity.invoke(mockFunctionOneException, spanName, attributes);
        } catch (Exception ex) {
            assertEquals(ex, e);
        }

        verify(mockTracer, times(1)).start(eq(spanName), eq(Context.NONE), eq(mockKind));
        verify(mockFunctionOneException, times(1)).apply(any());
        verify(mockTracer).end(eq("testException"), eq(e), eq(mockContext));

        reset(mockFunctionOneException);
        when(mockFunctionOneException.apply(any())).thenReturn("testResult");
        MonitoredActivity.invoke(mockFunctionOneException, spanName, attributes);

        verify(mockTracer, times(2)).start(eq(spanName), eq(Context.NONE), eq(mockKind));
        verify(mockFunctionOneException, times(1)).apply(any());
        verify(mockTracer).end(eq("success"), isNull(), eq(mockContext));

    }

    @Test
    public void testInvokeFunctionTwoExceptions() throws Exception {
        Tracer.initializeTracer(mockTracer);
        when(mockTracer.start(eq("testSpan"), eq(Context.NONE), eq(mockKind))).thenReturn(mockContext);
        FunctionTwoExceptions mockFunctionTwoExceptions = mock(FunctionTwoExceptions.class);
        Exception e = new Exception("testException");
        when(mockFunctionTwoExceptions.apply(any())).thenThrow(e);

        String spanName = "testSpan";
        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        try {
            MonitoredActivity.invoke(mockFunctionTwoExceptions, spanName, attributes);
        } catch (Exception ex) {
            assertEquals(ex, e);
        }

        verify(mockTracer, times(1)).start(eq(spanName), eq(Context.NONE), eq(mockKind));
        verify(mockFunctionTwoExceptions, times(1)).apply(any());
        verify(mockTracer).end(eq("testException"), eq(e), eq(mockContext));

        reset(mockFunctionTwoExceptions);
        when(mockFunctionTwoExceptions.apply(any())).thenReturn("testResult");
        MonitoredActivity.invoke(mockFunctionTwoExceptions, spanName, attributes);

        verify(mockTracer, times(2)).start(eq(spanName), eq(Context.NONE), eq(mockKind));
        verify(mockFunctionTwoExceptions, times(1)).apply(any());
        verify(mockTracer).end(eq("success"), isNull(), eq(mockContext));

    }

}
