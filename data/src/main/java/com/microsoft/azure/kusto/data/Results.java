// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import java.util.ArrayList;
import java.util.HashMap;

public class Results {
    private HashMap<String, Integer> columnNameToIndex;
    private HashMap<String, String> columnNameToType;
    private ArrayList<ArrayList<String>> values;
    private String exceptionsMessages;

    public Results(
            HashMap<String, Integer> columnNameToIndex,
            HashMap<String, String> columnNameToType,
            ArrayList<ArrayList<String>> values,
            String exceptionsMessages) {
        this.columnNameToIndex = columnNameToIndex;
        this.columnNameToType = columnNameToType;
        this.values = values;
        this.exceptionsMessages = exceptionsMessages;
    }

    public HashMap<String, Integer> getColumnNameToIndex() {
        return columnNameToIndex;
    }

    public HashMap<String, String> getColumnNameToType() {
        return columnNameToType;
    }

    public Integer getIndexByColumnName(String columnName) {
        return columnNameToIndex.get(columnName);
    }

    public String getTypeByColumnName(String columnName) {
        return columnNameToType.get(columnName);
    }

    public ArrayList<ArrayList<String>> getValues() {
        return values;
    }

    public String getExceptions() {
        return exceptionsMessages;
    }
}
