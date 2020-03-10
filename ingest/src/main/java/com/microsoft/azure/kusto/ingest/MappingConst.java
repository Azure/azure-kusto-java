package com.microsoft.azure.kusto.ingest;

public enum MappingConst {
    // Json Mapping consts
    PATH("Path"),
    TRANSFORMATION_METHOD("Transform"),
    // csv Mapping consts
    ORDINAL("Ordinal"),
    CONST_VALUE("ConstValue"),
    // Avro Mapping consts
    FIELD_NAME("Field"),
    COLUMNS("Columns"),
    STORAGE_DATA_TYPE("StorageDataType");

    private String name;

    MappingConst(String name) {
        this.name = name;
    }

    String getName() {
        return name;
    }
}

