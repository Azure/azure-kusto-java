// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Data class that describes the Mapping setting for .ingest command
 */
public class ColumnMapping implements Serializable {
    private final String columnName;
    private final String columnType;
    private final Map<String, String> properties;

    public ColumnMapping(String columnName, String cslDataType) {
        this(columnName, cslDataType, new HashMap<>());
    }

    public ColumnMapping(String columnName, String cslDataType, Map<String, String> properties) {
        this.columnName = columnName;
        this.columnType = cslDataType;
        this.properties = properties;
    }

    public ColumnMapping(ColumnMapping other) {
        columnName = other.columnName;
        columnType = other.columnType;
        properties = new HashMap<>(other.properties);
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setPath(String path) {
        properties.put(MappingConsts.PATH.getName(), path);
    }

    public String getPath() {
        return properties.get(MappingConsts.PATH.getName());
    }

    public void setTransform(TransformationMethod transform) {
        properties.put(MappingConsts.TRANSFORMATION_METHOD.getName(), transform.name());
    }

    public TransformationMethod getTransform() {
        String transform = properties.get(MappingConsts.TRANSFORMATION_METHOD.getName());
        return StringUtils.isEmpty(transform) ? null : TransformationMethod.valueOf(transform);
    }

    public void setOrdinal(Integer ordinal) {
        properties.put(MappingConsts.ORDINAL.getName(), String.valueOf(ordinal));
    }

    Integer getOrdinal() {
        String ordinal = properties.get(MappingConsts.ORDINAL.getName());
        return StringUtils.isEmpty(ordinal) ? null : Integer.valueOf(ordinal);
    }

    public void setConstantValue(String constValue) {
        properties.put(MappingConsts.CONST_VALUE.getName(), constValue);
    }

    public String getConstantValue() {
        return properties.get(MappingConsts.CONST_VALUE.getName());
    }

    public void setField(String field) {
        properties.put(MappingConsts.FIELD_NAME.getName(), field);
    }

    String setField() {
        return properties.get(MappingConsts.FIELD_NAME.getName());
    }

    public void setColumns(String columns) {
        properties.put(MappingConsts.COLUMNS.getName(), columns);
    }

    public String getColumns() {
        return properties.get(MappingConsts.COLUMNS.getName());
    }

    public void setStorageDataType(String dataType) {
        properties.put(MappingConsts.STORAGE_DATA_TYPE.getName(), dataType);
    }

    public String getStorageDataType() {
        return properties.get(MappingConsts.STORAGE_DATA_TYPE.getName());
    }

    public boolean isValid(IngestionMapping.IngestionMappingKind mappingKind)
    {
        switch (mappingKind)
        {
            case Csv:
            case SStream:
                return !StringUtils.isEmpty(this.columnName);
            case Json:
            case Parquet:
            case Orc:
            case W3CLogFile:
                TransformationMethod transformationMethod = getTransform();
                return !StringUtils.isEmpty(this.columnName) && (!StringUtils.isEmpty(getPath())
                        || transformationMethod == TransformationMethod.SourceLineNumber
                        || transformationMethod == TransformationMethod.SourceLocation);
            case Avro:
            case ApacheAvro:
                return !StringUtils.isEmpty(this.columnName) &&
                        !StringUtils.isEmpty(getColumns());
            default:
                return false;
        }
    }
}
