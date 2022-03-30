package com.microsoft.azure.kusto.data.exceptions;

public class ThrottleException extends DataServiceException {
    public static final String ERROR_MESSAGE = "Request was throttled, too many requests.";

    public ThrottleException(String ingestionSource) {
        super(ingestionSource, ERROR_MESSAGE, false);
    }
}
