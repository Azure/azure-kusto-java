package com.microsoft.azure.kusto.data.exceptions;

public class KustoParseException extends RuntimeException {

    public KustoParseException() {
        super();
    }

    public KustoParseException(String message) {
        super(message);
    }

}
