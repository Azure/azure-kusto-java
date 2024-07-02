package com.microsoft.azure.kusto.data.exceptions;

public class ExceptionsUtils {
    // Useful in IOException, where message might not propagate to the base IOException
    public static String getMessageEx(Exception e) {
        return (e.getMessage() == null && e.getCause() != null) ? e.getCause().getMessage() : e.getMessage();
    }
}
