// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import com.nimbusds.oauth2.sdk.util.CollectionUtils;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/*
  This class represents an error that returned from the service
 */
public class KustoServiceError extends Exception {
    private final List<Exception> exceptions;

    public KustoServiceError(JSONArray jsonExceptions, boolean isOneApi, String message) throws JSONException {
        this(message);
        for (int j = 0; j < jsonExceptions.length(); j++) {
            if (isOneApi) {
                this.exceptions.add(new DataWebException(jsonExceptions.getJSONObject(j).toString()));
            } else {
                this.exceptions.add(new Exception(jsonExceptions.getString(j)));
            }
        }
    }

    public KustoServiceError(String message) {
        super(message);
        this.exceptions = new ArrayList<>();
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    @Override
    public String toString() {
        return exceptions.isEmpty() ? getMessage() : "exceptions\":" + exceptions + "}";
    }

    public boolean isPermanent() {
        if (!CollectionUtils.isEmpty(exceptions) && exceptions.get(0) instanceof DataWebException) {
            return ((DataWebException) exceptions.get(0)).getApiError().isPermanent();
        }

        return false;
    }
}
