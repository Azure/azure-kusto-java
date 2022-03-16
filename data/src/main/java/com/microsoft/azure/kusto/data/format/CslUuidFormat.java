package com.microsoft.azure.kusto.data.format;

import com.microsoft.azure.kusto.data.Ensure;

import java.util.UUID;

public class CslUuidFormat extends CslFormat {
    private final UUID value;

    public CslUuidFormat(UUID value) {
        this.value = value;
    }

    @Override
    public String getType() {
        return "guid";
    }

    @Override
    public UUID getValue() {
        return value;
    }

    @Override
    String getValueAsString() {
        Ensure.argIsNotNull(value, "value");

        return value.toString();
    }
}