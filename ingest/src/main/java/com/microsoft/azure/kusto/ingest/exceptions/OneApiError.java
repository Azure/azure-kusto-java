// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.exceptions;

import com.microsoft.azure.kusto.data.exceptions.DataWebException;

import org.json.JSONException;
import org.json.JSONObject;

public class OneApiError {
    public OneApiError(String code, String message, String description, String type, String context, boolean permanent) {
        this.code = code;
        this.message = message;
        this.description = description;
        this.type = type;
        this.context = context;
        this.permanent = permanent;
    }

    public static OneApiError parseFromWebException(DataWebException ex) throws JSONException {
        JSONObject jsonObject = new JSONObject(ex.getMessage());
        return new OneApiError(
                jsonObject.getString("code"),
                jsonObject.getString("message"),
                jsonObject.getString("@message"),
                jsonObject.getString("@type"),
                jsonObject.getString("@context"),
                jsonObject.getBoolean("@permanent")
        );
    }

    private final String code;
    private final String message;
    private final String description;
    private final String type;
    private final String context;
    private final boolean permanent;

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public String getDescription() {
        return description;
    }

    public String getType() {
        return type;
    }

    public String getContext() {
        return context;
    }

    public boolean isPermanent() {
        return permanent;
    }
}
