// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;

/*
 This class represents an error that returned from the query result
*/
public class KustoServiceQueryError extends Exception {
    private final List<Exception> exceptions;

    public KustoServiceQueryError(JSONArray jsonExceptions, boolean isOneApi, String message) throws JSONException {
        super(message);
        this.exceptions = new ArrayList<>();
        for (int j = 0; j < jsonExceptions.length(); j++) {
            if (isOneApi) {
                this.exceptions.add(
                        new DataWebException(jsonExceptions.getJSONObject(j).toString()));
            } else {
                this.exceptions.add(new Exception(jsonExceptions.getString(j)));
            }
        }
    }

    public KustoServiceQueryError(String message) {
        super(message);
        this.exceptions = new ArrayList<>();
        this.exceptions.add(new Exception(message));
    }

    public List<Exception> getExceptions() {
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
