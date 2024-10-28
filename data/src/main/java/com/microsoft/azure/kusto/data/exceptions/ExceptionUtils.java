package com.microsoft.azure.kusto.data.exceptions;

import java.io.IOException;
import java.io.UncheckedIOException;

public class ExceptionUtils {
    public static IOException tryCastToIOException(Exception e) throws RuntimeException {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        if (e instanceof UncheckedIOException) {
            return ((UncheckedIOException) e).getCause();
        }

        throw (RuntimeException)e;
    }
}
