// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.format;

import com.microsoft.azure.kusto.data.Ensure;

public class CslIntFormat extends CslFormat {
    private final Integer value;

    public CslIntFormat(int value) {
        this.value = value;
    }

    @Override
    public String getType() {
        return "int";
    }

    @Override
    public Integer getValue() {
        return value;
    }

    @Override
    String getValueAsString() {
        Ensure.argIsNotNull(value, "value");

        return Integer.toString(value);
    }
}
