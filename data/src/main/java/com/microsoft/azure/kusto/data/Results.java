package com.microsoft.azure.kusto.data;

import java.util.*;

public class Results {
    private final Map<String, Integer> columnNameToIndex;
    private final List<String> headers;
    private final Map<String, String> columnNameToType;
    private final List<List<String>> values;
    private final String exceptionsMessages;

    public Results(Map<String, Integer> columnNameToIndex,
                   List<String> headers,
                   Map<String, String> columnNameToType,
                   List<List<String>> values,
                   String exceptionsMessages) {
        this.columnNameToIndex = columnNameToIndex;
        this.headers = headers;
        this.columnNameToType = columnNameToType;
        this.values = values;
        this.exceptionsMessages = exceptionsMessages;
    }

    public Map<String, Integer> getColumnNameToIndex() {
        return columnNameToIndex;
    }

    public Map<String, String> getColumnNameToType() {
        return columnNameToType;
    }

    public Integer getIndexByColumnName(String columnName) {
        return columnNameToIndex.get(columnName);
    }

    public String getTypeByColumnName(String columnName) {
        return columnNameToType.get(columnName);
    }

    public List<List<String>> getValues() {
        return values;
    }

    public String getExceptions() {
        return exceptionsMessages;
    }

    public List<String> getHeaders(){
       return headers;
    }
}
