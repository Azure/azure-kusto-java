// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

public class KustoResultColumn {
    private String columnName;
    private String columnType;
    private int ordinal;

    public KustoResultColumn(String columnName, String columnType, int ordinal) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.ordinal = ordinal;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public int getOrdinal() {
        return ordinal;
    }
}
