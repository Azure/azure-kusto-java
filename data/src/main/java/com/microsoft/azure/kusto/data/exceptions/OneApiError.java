// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import org.json.JSONObject;

public class OneApiError {
    public OneApiError(String code, String message, String description, String type, JSONObject context, boolean permanent) {
        this.code = code;
        this.message = message;
        this.description = description;
        this.type = type;
        this.context = context;
        this.permanent = permanent;
    }

    public static OneApiError fromJsonObject(JSONObject jsonObject) {
        return new OneApiError(
                jsonObject.getString("code"),
                jsonObject.getString("message"),
                jsonObject.getString("@message"),
                jsonObject.getString("@type"),
                jsonObject.getJSONObject("@context"),
                jsonObject.getBoolean("@permanent"));
    }

    private final String code;
    private final String message;
    private final String description;
    private final String type;
    private final JSONObject context;
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

    public JSONObject getContext() {
        return context;
    }

    public boolean isPermanent() {
        return permanent;
    }
}
