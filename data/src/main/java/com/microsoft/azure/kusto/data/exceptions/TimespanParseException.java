package com.microsoft.azure.kusto.data.exceptions;

public class TimespanParseException extends RuntimeException {
    static final String formatError = "Failed to parse timeout string as a timespan. Value: %s";
    public TimespanParseException(String value){
        super(String.format(formatError, value));
    }
}
