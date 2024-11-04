package com.microsoft.azure.kusto.data.exceptions;

import com.microsoft.azure.kusto.data.Utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;

public class ExceptionUtils {
    public static DataServiceException createExceptionOnPost(Exception e, URL url, String kind) {
        boolean permanent = false;
        String prefix = "";
        boolean isIO = false;
        if (e instanceof IOException) {
            isIO = true;
        }

        if (e instanceof UncheckedIOException) {
            e = ((UncheckedIOException) e).getCause();
            isIO = true;
        }

        if (isIO) {
            return new IODataServiceException(url.toString(), e);

        }

        return new DataServiceException(url.toString(), String.format("%sException in %s post request: %s", prefix, kind, e.getMessage()), permanent);
    }
}
