// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Utils;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class DataWebException extends WebException {
    private OneApiError apiError = null;

    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ObjectMapper objectMapper = Utils.getObjectMapper();

    public DataWebException(String message, HttpResponse httpResponse, Throwable cause) {
        super(message, httpResponse, cause);
    }

    public DataWebException(String message, HttpResponse httpResponse) {
        this(message, httpResponse, null);
    }

    public DataWebException(String message) {
        this(message, null, null);
    }

    public OneApiError getApiError() {
        if (apiError == null) {
            try {
                apiError = OneApiError.fromJsonObject(objectMapper.readTree(getMessage()).get("error"));
            } catch (JsonProcessingException e) {
                log.error("failed to parse error from message {} {} " , e.getMessage(), e);
            }
        }
        return apiError;
    }
}
