package com.microsoft.azure.kusto.data.exceptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;

public class ExceptionUtils {
    public static DataServiceException createExceptionOnPost(Exception e, URL url, String kind) {
        boolean permanent = false;
        boolean isIO = false;
        if (e instanceof IOException) {
            isIO = true;
        }

        if (e instanceof UncheckedIOException) {
            e = ((UncheckedIOException) e).getCause();
            isIO = true;
        }

        if (isIO) {
            return new IODataServiceException(url.toString(), (IOException) e, kind);

        }

        return new DataServiceException(url.toString(), String.format("Exception in %s post request: %s", kind, e.getMessage()), permanent);
    }
}
