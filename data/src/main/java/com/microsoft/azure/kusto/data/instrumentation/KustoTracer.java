package com.microsoft.azure.kusto.data.instrumentation;

import com.azure.core.util.Configuration;
import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.azure.core.util.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

public class KustoTracer {
    private static final boolean IS_TRACING_DISABLED = Configuration.getGlobalConfiguration().get(Configuration.PROPERTY_AZURE_TRACING_DISABLED, false);
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static Tracer tracer;
    private static volatile boolean initialized = false;



    public static void initializeTracer(Tracer tracer){
        if (!KustoTracer.initialized){
            synchronized (KustoTracer.class){
                if (!KustoTracer.initialized){
                    KustoTracer.tracer = IS_TRACING_DISABLED ? null : tracer;
                    initialized = true;
                }
            }
        }
    }

    public static KustoSpan startSpan(String spanName, Context context, ProcessKind kind, Map<String, String> attributes) {
        Context span = tracer == null ? context : tracer.start(spanName, context, kind);
        setAttributes(attributes, span);
        return new KustoSpan(span);
    }

    public static void endSpan(Throwable throwable, Context span, AutoCloseable scope) {
        if (tracer != null) {
            String errorCondition = "success";
            if (throwable != null) {
                errorCondition = throwable.getLocalizedMessage();
            }
            try {
                if (scope != null) {
                    scope.close();
                }
            } catch (Exception e) {
                log.warn("Can't close scope", e);
            } finally {
                tracer.end(errorCondition, throwable, span);
            }
        }
    }
    public static void setAttributes(Map<String, String> attributes, Context span){
        if (attributes != null){
            attributes.forEach((k, v) -> tracer.setAttribute(k, v, span));
        }
    }

    public static class KustoSpan implements AutoCloseable{
        private final Context span;
        private Throwable throwable;
        public KustoSpan(Context span) {
            this.span = span;
        }

        @Override
        public void close() {
            endSpan(throwable, span, null);
        }

        public void addException(Exception e) {
            throwable = e;
        }
    }
}
