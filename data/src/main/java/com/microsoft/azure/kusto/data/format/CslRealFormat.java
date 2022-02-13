// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.format;

import com.microsoft.azure.kusto.data.Ensure;

public class CslRealFormat extends CslFormat {
    private final Double value;

    public CslRealFormat(double value) {
        this.value = value;
    }

    @Override
    public String getType() {
        return "real";
    }

    @Override
    public Double getValue() {
        return value;
    }

    @Override
    String getValueAsString() {
        Ensure.argIsNotNull(value, "value");

        return Double.toString(value);
    }
}
