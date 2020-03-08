package com.microsoft.azure.kusto.ingest;

import java.io.Serializable;
import java.util.Map;

/**
 * Data class that describes the Mapping setting for .ingest command
 */
public class ColumnMapping implements Serializable {
    public String columnName;
    public String columnType;
    public Map<String, String> properties;

    public ColumnMapping(String columnName, String cslDataType, Map properties) {
        this.columnName = columnName;
        this.columnType = cslDataType;
        this.properties = properties;
    }

    public void addProperty(MappingConst propertyName, String value){
        if (propertyName != null){
            properties.put(propertyName.getName(), value);
        }
    }

    public String getProperty(MappingConst propertyName){
        String propertyValue = properties.get(propertyName.getName());
        return propertyValue == null ? "" : propertyValue;
    }
}
