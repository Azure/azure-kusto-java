package com.microsoft.azure.kusto.data.format;

import com.microsoft.azure.kusto.data.Ensure;

public class CslLongFormat extends CslFormat {
    private final Long value;

    public CslLongFormat(long value) {
        this.value = value;
    }

    @Override
    public String getType() {
        return "long";
    }

    @Override
    public Long getValue() {
        return value;
    }

    @Override
    String getValueAsString() {
        Ensure.argIsNotNull(value, "value");

        return Long.toString(value);
    }
}