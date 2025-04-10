// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class OneApiError {
    public OneApiError(String code, String message, String description, String type, JsonNode context, boolean permanent) {
        this.code = code;
        this.message = message;
        this.description = description;
        this.type = type;
        this.context = context;
        this.permanent = permanent;
    }

    public static OneApiError[] fromJsonArray(ArrayNode array) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(array.elements(), Spliterator.ORDERED), false)
                .map(x -> OneApiError.fromJsonObject(x.get("error")))
                .toArray(OneApiError[]::new);
    }

    public static OneApiError fromJsonObject(JsonNode jsonObject) {
        return new OneApiError(
                jsonObject.has("code") ? jsonObject.get("code").asText() : "",
                jsonObject.has("message") ? jsonObject.get("message").asText() : "",
                jsonObject.has("@message") ? jsonObject.get("@message").asText() : "",
                jsonObject.has("@type") ? jsonObject.get("@type").asText() : "",
                jsonObject.get("@context"),
                jsonObject.has("@permanent") && jsonObject.get("@permanent").asBoolean());
    }

    private final String code;
    private final String message;
    private final String description;
    private final String type;
    private final JsonNode context;
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

    public JsonNode getContext() {
        return context;
    }

    public boolean isPermanent() {
        return permanent;
    }
}
