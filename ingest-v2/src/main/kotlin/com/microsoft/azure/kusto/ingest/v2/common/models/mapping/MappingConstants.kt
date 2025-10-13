// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models.mapping

import kotlinx.serialization.Serializable as KSerializable

@KSerializable
enum class MappingConstants(val value: String) {
    // Json Mapping constants
    Path("Path"),
    Transform("Transform"),

    // csv Mapping constants
    Ordinal("Ordinal"),
    ConstValue("ConstValue"),

    // Avro Mapping constants
    Field("Field"),
    Columns("Columns"),

    // General Mapping constants
    StorageDataType("StorageDataType"),
}
