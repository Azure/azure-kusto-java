package com.microsoft.azure.kusto.ingest;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract class to column mapping.
 */
public abstract class ColumnMapping implements Serializable {
    private String columnName;
    private String cslDataType;
    private Map Properties;

    public ColumnMapping(String columnName, String cslDataType, Integer ordinal) {
        this.columnName = columnName;
        this.cslDataType = cslDataType;
        this.Properties = new HashMap<String, String>();
    }
}
