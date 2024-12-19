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
    private final List<RuntimeException> exceptions;

    public KustoServiceQueryError(ArrayNode jsonExceptions, boolean isOneApi, String message) {
        super(message);
        this.exceptions = new ArrayList<>();
        for (int j = 0; jsonExceptions != null && j < jsonExceptions.size(); j++) {
            if (isOneApi) {
                this.exceptions.add(new DataWebException(jsonExceptions.get(j).toString()));
            } else {
                this.exceptions.add(new RuntimeException(jsonExceptions.get(j).toString()));
            }
        }
    }

    public KustoServiceQueryError(String message) {
        super(message);
        this.exceptions = new ArrayList<>();
        this.exceptions.add(new RuntimeException(message));
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
