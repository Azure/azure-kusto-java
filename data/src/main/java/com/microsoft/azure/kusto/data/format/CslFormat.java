package com.microsoft.azure.kusto.data.format;

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
}