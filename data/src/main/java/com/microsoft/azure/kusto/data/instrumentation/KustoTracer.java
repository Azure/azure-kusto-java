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
    private static  Tracer tracer;
    private static KustoTracer instance = new KustoTracer(null);
    private static volatile boolean initialized = false;

    private KustoTracer(Tracer tracer){
        KustoTracer.tracer = IS_TRACING_DISABLED ? null : tracer;
    }

    public static void initializeTracer(Tracer tracer){
        if (!KustoTracer.initialized){
            synchronized (KustoTracer.class){
                if (!KustoTracer.initialized){
                    instance = new KustoTracer(tracer);
                    initialized = true;
                }
            }
        }
    }
    public static KustoTracer getInstance(){return instance;}

    public Context startSpan(String spanName, Context context, ProcessKind kind) {
        return tracer == null ? context : tracer.start(spanName, context, kind);
    }

    public void endSpan(Throwable throwable, Context span, AutoCloseable scope) {
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
    public void setAttributes(Map<String, String> attributes, Context span){
        attributes.forEach((k, v) -> tracer.setAttribute(k, v, span));
    }
//    public void getCurrentSpan(){
//        return tracer.
//    }
}
