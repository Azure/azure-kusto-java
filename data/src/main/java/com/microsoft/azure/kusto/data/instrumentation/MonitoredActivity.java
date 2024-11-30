package com.microsoft.azure.kusto.data.instrumentation;

import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

public class MonitoredActivity {
    public static void invoke(Runnable runnable, String nameOfSpan) {
        invoke(runnable, nameOfSpan, new HashMap<>());
    }

    public static void invoke(Runnable runnable, String nameOfSpan, Map<String, String> attributes) {
        try (Tracer.Span ignored = Tracer.startSpan(nameOfSpan, attributes)) {
            runnable.run();
        }
    }

    public static <T> T invoke(SupplierNoException<T> supplier, String nameOfSpan) {
        return invoke(supplier, nameOfSpan, new HashMap<>());
    }

    public static <T> T invoke(SupplierNoException<T> supplier, String nameOfSpan, Map<String, String> attributes) {
        try (Tracer.Span span = Tracer.startSpan(nameOfSpan, attributes)) {
            return supplier.get();
        }
    }

    public static <T, U extends Exception> T invoke(SupplierOneException<T, U> supplier, String nameOfSpan) throws U {
        return invoke((SupplierTwoExceptions<T, U, U>) supplier::get, nameOfSpan, new HashMap<>());
    }

    public static <T> Mono<T> wrap(Mono<T> mono, String nameOfSpan) {
        return Mono.fromCallable(() -> Tracer.startSpan(nameOfSpan, new HashMap<>()))
                .flatMap(span -> mono.doOnTerminate(span::close));
    }

    public static <T> Mono<T> wrap(Mono<T> mono, String nameOfSpan, Map<String, String> attributes) {
        return Mono.fromCallable(() -> Tracer.startSpan(nameOfSpan, attributes))
                .flatMap(span -> mono.doOnTerminate(span::close));
    }

    public static <T, U extends Exception> Mono<T> invokeAsync(FunctionOneException<Mono<T>, Tracer.Span, U> function,
                                                               String nameOfSpan,
                                                               Map<String, String> attributes) {
        return Mono.defer(() -> {
            Tracer.Span span = Tracer.startSpan(nameOfSpan, attributes);
            try {
                return function.apply(span)
                        .doOnSuccess(ignored -> span.close())
                        .doOnError(e -> span.addException((Exception) e));
            } catch (Exception e) {
                span.addException(e);
                return Mono.error(e);
            }
        });
    }

    public static <T, U extends Exception> T invoke(SupplierOneException<T, U> supplier, String nameOfSpan, Map<String, String> attributes) throws U {
        return invoke((SupplierTwoExceptions<T, U, U>) supplier::get, nameOfSpan, attributes);
    }

    public static <T, U1 extends Exception, U2 extends Exception> T invoke(SupplierTwoExceptions<T, U1, U2> supplier, String nameOfSpan) throws U1, U2 {
        return invoke(supplier, nameOfSpan, new HashMap<>());
    }

    public static <T, U1 extends Exception, U2 extends Exception> T invoke(SupplierTwoExceptions<T, U1, U2> supplier, String nameOfSpan,
            Map<String, String> attributes) throws U1, U2 {
        try (Tracer.Span span = Tracer.startSpan(nameOfSpan, attributes)) {
            try {
                return supplier.get();
            } catch (Exception e) {
                span.addException(e);
                throw e;
            }
        }
    }

    public static <T, U extends Exception> T invoke(FunctionOneException<T, Tracer.Span, U> function, String nameOfSpan) throws U {
        return invoke((FunctionTwoExceptions<T, Tracer.Span, U, U>) function::apply, nameOfSpan, new HashMap<>());
    }

    public static <T, U extends Exception> T invoke(FunctionOneException<T, Tracer.Span, U> function, String nameOfSpan, Map<String, String> attributes)
            throws U {
        return invoke((FunctionTwoExceptions<T, Tracer.Span, U, U>) function::apply, nameOfSpan, attributes);
    }

    public static <T, U1 extends Exception, U2 extends Exception> T invoke(FunctionTwoExceptions<T, Tracer.Span, U1, U2> function, String nameOfSpan)
            throws U1, U2 {
        return invoke(function, nameOfSpan, new HashMap<>());
    }

    public static <T, U1 extends Exception, U2 extends Exception> T invoke(FunctionTwoExceptions<T, Tracer.Span, U1, U2> function, String nameOfSpan,
            Map<String, String> attributes) throws U1, U2 {
        try (Tracer.Span span = Tracer.startSpan(nameOfSpan, attributes)) {
            try {
                return function.apply(span);
            } catch (Exception e) {
                span.addException(e);
                throw e;
            }
        }

    }
}
