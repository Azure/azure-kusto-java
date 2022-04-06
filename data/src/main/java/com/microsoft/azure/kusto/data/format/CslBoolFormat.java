package com.microsoft.azure.kusto.data.format;

import com.microsoft.azure.kusto.data.Ensure;

public class CslBoolFormat extends CslFormat {
    private final Boolean value;

    public CslBoolFormat(boolean value) {
        this.value = value;
    }

    @Override
    public String getType() {
        return "bool";
    }

    @Override
    public Boolean getValue() {
        return value;
    }

    @Override
    String getValueAsString() {
        Ensure.argIsNotNull(value, "value");

        return Boolean.TRUE.equals(value) ? "true" : "false";
    }
}
