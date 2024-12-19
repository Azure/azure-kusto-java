package com.microsoft.azure.kusto.data.exceptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;

import com.microsoft.azure.kusto.data.Utils;

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

    // Useful in IOException, where message might not propagate to the base IOException
    public static String getMessageEx(Exception e) {
        return (e.getMessage() == null && e.getCause() != null) ? e.getCause().getMessage() : e.getMessage();
    }

    public static Exception unwrapCloudInfoException(String clusterUrl, Throwable throwable) {
        if (throwable instanceof URISyntaxException) {
            return new DataServiceException(clusterUrl, "URISyntaxException when trying to retrieve cluster metadata: " + throwable.getMessage(),
                    (URISyntaxException) throwable, true);
        }

        if (throwable instanceof IOException) {
            IOException ex = (IOException) throwable;
            if (!Utils.isRetriableIOException(ex)) {
                return new DataServiceException(clusterUrl, "IOException when trying to retrieve cluster metadata: " + getMessageEx(ex),
                        ex,
                        Utils.isRetriableIOException(ex));
            }
        }

        if (throwable instanceof DataServiceException) {
            DataServiceException e = (DataServiceException) throwable;
            if (e.isPermanent()) {
                return e;
            }
        }

        return new DataClientException(clusterUrl, throwable.toString(), null);
    }
}
