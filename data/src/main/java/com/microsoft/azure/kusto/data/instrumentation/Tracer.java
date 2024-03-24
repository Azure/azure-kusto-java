package com.microsoft.azure.kusto.data.instrumentation;

import com.azure.core.util.Configuration;
import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;

import java.util.Map;

public class Tracer {
    private static final boolean IS_TRACING_DISABLED = Configuration.getGlobalConfiguration().get(Configuration.PROPERTY_AZURE_TRACING_DISABLED, false);
    private static com.azure.core.util.tracing.Tracer tracer;
    private static volatile boolean initialized = false;

    public static void initializeTracer(com.azure.core.util.tracing.Tracer tracer) {
        if (!Tracer.initialized) {
            synchronized (Tracer.class) {
                if (!Tracer.initialized) {
                    Tracer.tracer = IS_TRACING_DISABLED ? null : tracer;
                    initialized = true;
                }
            }
        }
    }

    public static Span startSpan(String spanName) {
        return startSpan(spanName, Context.NONE, ProcessKind.PROCESS, null);
    }

    public static Span startSpan(String spanName, Map<String, String> attributes) {
        return startSpan(spanName, Context.NONE, ProcessKind.PROCESS, attributes);
    }

    public static Span startSpan(String spanName, Context context, ProcessKind kind, Map<String, String> attributes) {
        Context span = tracer == null ? null : tracer.start(spanName, context, kind);
        return new Span(span, attributes);
    }

    public static class Span implements AutoCloseable {
        private final Context span;
        private Throwable throwable;

        private Span(Context span, Map<String, String> attributes) {
            this.span = span;
            setAttributes(attributes);
        }

        @Override
        public void close() {
            if (tracer != null && span != null) {
                String errorCondition = "success";
                if (throwable != null && throwable.getMessage() != null) {
                    errorCondition = throwable.getLocalizedMessage();
                }
                tracer.end(errorCondition, throwable, span);
            }
        }

        public void addException(Exception e) {
            throwable = e;
        }

        public void setAttributes(Map<String, String> attributes) {
            if (tracer != null && attributes != null && span != null) {
                attributes.forEach((k, v) -> tracer.setAttribute(k, v, span));
            }
        }
    }
}
