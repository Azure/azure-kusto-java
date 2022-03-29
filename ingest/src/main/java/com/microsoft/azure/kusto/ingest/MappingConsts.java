// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

public enum MappingConsts {
    // Json Mapping consts
    PATH("Path"),
    TRANSFORMATION_METHOD("Transform"),
    // csv Mapping consts
    ORDINAL("Ordinal"),
    CONST_VALUE("ConstValue"),
    // Avro Mapping consts
    FIELD_NAME("Field"),
    COLUMNS("Columns"),
    // General Mapping consts
    STORAGE_DATA_TYPE("StorageDataType");

    private String name;

    MappingConsts(String name) {
        this.name = name;
    }

    String getName() {
        return name;
    }
}
