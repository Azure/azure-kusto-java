// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.format;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class CslFormat {
    public abstract String getType();

    public abstract Object getValue();

    // Value must not be null
    abstract String getValueAsString();

    private String getValueOrNullAsString() {
        if (getValue() == null) {
            return "null";
        } else {
            return getValueAsString();
        }
    }

    public String toString() {
        return getType() + "(" + getValueOrNullAsString() + ")";
    }

    // For example, parses "int(7)" as "7"
    // If type wasn't found in valueWithType, returns it unchanged
    public static String parseValueFromValueWithType(String valueWithType, String type) {
        String valueWithTypeRegex = String.format("%s\\s*\\(\\s*(.*\\S+)\\s*\\)\\s*", type);
        Pattern valueWithTypePattern = Pattern.compile(valueWithTypeRegex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = valueWithTypePattern.matcher(valueWithType);
        return matcher.matches() ? matcher.group(1) : valueWithType;
    }
}
