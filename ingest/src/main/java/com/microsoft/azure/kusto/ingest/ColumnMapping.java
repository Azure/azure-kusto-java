package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.source.TransformationMethod;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Data class that describes the Mapping setting for .ingest command
 */
public class ColumnMapping implements Serializable {
    public String columnName;
    public String columnType;
    public Map<String, String> properties;

    public ColumnMapping(String columnName, String cslDataType) {
        this(columnName, cslDataType, new HashMap<>());
    }

    public ColumnMapping(String columnName, String cslDataType, Map<String, String> properties) {
        this.columnName = columnName;
        this.columnType = cslDataType;
        this.properties = properties;
    }

    public void setJsonPath(String path) {
        properties.put(MappingConst.PATH.getName(), path);
    }

    public String getJsonPath() {
        return properties.get(MappingConst.PATH.getName());
    }

    public void setJsonTransform(TransformationMethod transform) {
        properties.put(MappingConst.TRANSFORMATION_METHOD.getName(), transform.name());
    }

    public TransformationMethod getJsonTransform() {
        String transform = properties.get(MappingConst.TRANSFORMATION_METHOD.getName());
        return StringUtils.isEmpty(transform) ? null : TransformationMethod.valueOf(transform);
    }

    public void setCsvOrdinal(Integer ordinal) {
        properties.put(MappingConst.ORDINAL.getName(), String.valueOf(ordinal));
    }

    Integer getCsvOrdinal() {
        String ordinal = properties.get(MappingConst.ORDINAL.getName());
        return StringUtils.isEmpty(ordinal) ? null : Integer.valueOf(ordinal);
    }

    public void setCsvConstantValue(String constValue) {
        properties.put(MappingConst.CONST_VALUE.getName(), constValue);
    }

    public String getCsvConstantValue() {
        return properties.get(MappingConst.CONST_VALUE.getName());
    }

    public void setAvroField(String field) {
        properties.put(MappingConst.FIELD_NAME.getName(), field);
    }

    String setAvroField() {
        return properties.get(MappingConst.FIELD_NAME.getName());
    }

    public void setAvroColumns(String columns) {
        properties.put(MappingConst.COLUMNS.getName(), columns);
    }

    public String getAvroColumns() {
        return properties.get(MappingConst.COLUMNS.getName());
    }

    public void setStorageDataType(String dataType) {
        properties.put(MappingConst.STORAGE_DATA_TYPE.getName(), dataType);
    }

    public String getStorageDataType() {
        return properties.get(MappingConst.STORAGE_DATA_TYPE.getName());
    }

    public boolean isValid(IngestionMapping.IngestionMappingKind mappingKind)
    {
        switch (mappingKind)
        {
            case Csv:
                return !StringUtils.isEmpty(this.columnName);
            case Json:
            case Parquet:
            case Orc:
                TransformationMethod transformationMethod = getJsonTransform();
                return !StringUtils.isEmpty(this.columnName) && (!StringUtils.isEmpty(getJsonPath())
                        || transformationMethod == TransformationMethod.SourceLineNumber
                        || transformationMethod == TransformationMethod.SourceLocation);
            case Avro:
                return !StringUtils.isEmpty(this.columnName) &&
                        !StringUtils.isEmpty(getAvroColumns());
            default:
                return false;
        }
    }
}
