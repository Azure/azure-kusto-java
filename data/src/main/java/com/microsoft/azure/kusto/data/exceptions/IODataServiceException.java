package com.microsoft.azure.kusto.data.exceptions;

import com.microsoft.azure.kusto.data.Utils;

import java.io.IOException;

public class IODataServiceException extends DataServiceException {
    public IODataServiceException(String ingestionSource, IOException e, String kind) {
        super(ingestionSource, String.format("IOException in %s post request: %s", kind, e.getMessage()),
                e,
                !Utils.isRetriableIOException(e));
    }
}
