// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/*
  This class represents an error that returned from the service
 */
public class KustoServiceError extends Exception {
    private ArrayList<Exception> exceptions;

    public KustoServiceError(JSONArray jsonExceptions, boolean isOneApi, String message) throws JSONException {
        this(message);
        this.exceptions = new ArrayList<>();
        if (isOneApi){
            for (int j = 0; j < jsonExceptions.length(); j++) {
                this.exceptions.add(new DataWebException(jsonExceptions.getJSONObject(j).toString()));
            }
        } else {
            for (int j = 0; j < jsonExceptions.length(); j++) {
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
        return exceptions.size() == 0 ? getMessage() : "exceptions\":" + exceptions + "}";
    }


    public TriState isPermanent(){
        if (exceptions.size() > 0 && exceptions.get(0) instanceof DataWebException) {
            return KustoClientException.triStateFromBool(((DataWebException) exceptions.get(0)).getApiError().isPermanent());
        }

        return TriState.DONTKNOW;
    }
}
