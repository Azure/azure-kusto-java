package com.microsoft.azure.kusto.data.exceptions;

import com.microsoft.azure.kusto.data.Utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
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

    public static Exception unwrapCloudInfoException(String clusterUrl, Throwable throwable) {
        if (throwable instanceof URISyntaxException) {
            return new DataServiceException(clusterUrl, "URISyntaxException when trying to retrieve cluster metadata:" + throwable.getMessage(),
                    (URISyntaxException) throwable, true);
        }

        if (throwable instanceof IOException) {
            IOException ex = (IOException) throwable;
            if (!Utils.isRetriableIOException(ex)) {
                return new DataServiceException(clusterUrl, "IOException when trying to retrieve cluster metadata:" + ExceptionsUtils.getMessageEx(ex),
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
