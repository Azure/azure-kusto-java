package com.microsoft.azure.kusto.data.instrumentation;

import com.azure.core.util.Configuration;
import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.azure.core.util.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

public class DistributedTracing {
    private static final boolean IS_TRACING_DISABLED = Configuration.getGlobalConfiguration().get(Configuration.PROPERTY_AZURE_TRACING_DISABLED, false);
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static Tracer tracer;
    private static volatile boolean initialized = false;
    
    public static void initializeTracer(Tracer tracer) {
        if (!DistributedTracing.initialized) {
            synchronized (DistributedTracing.class) {
                if (!DistributedTracing.initialized) {
                    DistributedTracing.tracer = IS_TRACING_DISABLED ? null : tracer;
                    initialized = true;
                }
            }
        }
    }

    public static Span startSpan(String spanName, Context context, ProcessKind kind, Map<String, String> attributes) {
        Context span = tracer == null ? context : tracer.start(spanName, context, kind);
        return new Span(span, attributes);
    }

    public static class Span implements AutoCloseable{
        private final Context span;
        private Throwable throwable;
        private Span(Context span, Map<String, String> attributes) {
            this.span = span;
            Map<String, String> map = new HashMap<>();
            map.put("thread", String.valueOf(ProcessHandle.current().pid()));
            setAttributes(map);
            setAttributes(attributes);
            }

        @Override
        public void close() {
            if (tracer != null) {
                String errorCondition = "success";
                if (throwable != null) {
                    errorCondition = throwable.getLocalizedMessage();
                }
    //                try {
    //                    if (scope != null) {
    //                        scope.close();
    //                    }
    //                } catch (Exception e) {
    //                    log.warn("Can't close scope", e);
    //                } finally {
    //
    //                }
                tracer.end(errorCondition, throwable, span);
            }
        }

        public void addException(Exception e) {
            throwable = e;
        }

        public void setAttributes(Map<String, String> attributes){
            if (attributes != null){
                attributes.forEach((k, v) -> tracer.setAttribute(k, v, span));
            }
        }
    }
}
