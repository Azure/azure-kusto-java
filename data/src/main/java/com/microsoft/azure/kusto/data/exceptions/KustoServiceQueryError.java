// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import java.util.ArrayList;
import java.util.List;

import com.azure.core.exception.AzureException;
import com.fasterxml.jackson.databind.node.ArrayNode;

/*
  This class represents an error that returned from the query result
 */
public class KustoServiceQueryError extends AzureException {
    static final String EXCEPTIONS_MESSAGE = "Query execution failed with multiple inner exceptions:\n";

    private final List<RuntimeException> exceptions;

    public KustoServiceQueryError(String message, List<RuntimeException> exceptions) {
        super(message);
        this.exceptions = exceptions;
    }

    public KustoServiceQueryError(String message) {
        super(message);
        this.exceptions = new ArrayList<>();
        this.exceptions.add(new RuntimeException(message));
    }

    public static KustoServiceQueryError fromOneApiErrorArray(ArrayNode jsonExceptions, boolean isOneApi) {
        List<RuntimeException> exceptions = new ArrayList<>();
        StringBuilder sb = new StringBuilder();

        if (jsonExceptions == null || jsonExceptions.isEmpty()) {
            return new KustoServiceQueryError("No exceptions were returned from the service.");
        }

        if (jsonExceptions.size() > 1) {
            sb.append(EXCEPTIONS_MESSAGE);
        }

        for (int i = 0; i < jsonExceptions.size(); i++) {
            if (isOneApi) {
                exceptions.add(new DataWebException(jsonExceptions.get(i).toString()));
            } else {
                exceptions.add(new RuntimeException(jsonExceptions.get(i).toString()));
            }
            sb.append(jsonExceptions.get(i).toString());
            sb.append("\n");
        }

        return new KustoServiceQueryError(sb.toString(), exceptions);
    }

    public List<RuntimeException> getExceptions() {
        return exceptions;
    }

    @Override
    public String toString() {
        return exceptions.isEmpty() ? getMessage() : "exceptions\":" + exceptions + "}";
    }

    public boolean isPermanent() {
        if (exceptions.size() > 0 && exceptions.get(0) instanceof DataWebException) {
            return ((DataWebException) exceptions.get(0)).getApiError().isPermanent();
        }

        return false;
    }
}
