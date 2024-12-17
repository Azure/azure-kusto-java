package com.microsoft.azure.kusto.data.exceptions;

import com.azure.core.exception.AzureException;

public class ParseException extends AzureException {

    public ParseException(String message) {
        super(message);
    }
}
