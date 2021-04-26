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
    private ArrayList<Exception> exceptions = null;

    public KustoServiceError(JSONArray exceptions, boolean isOneApi, String message) throws JSONException {
        this(message);
        this.exceptions = new ArrayList<>();
        if (isOneApi){
            for (int j = 0; j < exceptions.length(); j++) {
                this.exceptions.add(new DataWebException(exceptions.getJSONObject(j).toString(), null));
            }
        } else {
            for (int j = 0; j < exceptions.length(); j++) {
                this.exceptions.add(new Exception(exceptions.getString(j)));
            }
        }
    }

    public KustoServiceError(String message) {
        super(message);
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    @Override
    public String toString() {
        return "{\"exceptions\":" + exceptions + "}";
    }

    /*
      Can return null if permanency is not known
     */
    public Boolean isPermanent(){
        if (exceptions != null && exceptions.size() > 0 && exceptions.get(0) instanceof DataWebException) {
            return ((DataWebException) exceptions.get(0)).getApiError().isPermanent();
        }

        return null;
    }
}
