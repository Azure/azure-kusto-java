// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.utils

import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType

object IngestionUtils {
    fun getRowStoreEstimatedFactor(
        format: Format?,
        compressionType: CompressionType,
    ): Double {
        val isCompressed = compressionType != CompressionType.NONE
        val fmt = format ?: Format.csv
        return when {
            !isCompressed && fmt == Format.avro -> 0.55
            !isCompressed && fmt == Format.apacheavro -> 0.55
            !isCompressed && fmt == Format.csv -> 0.45
            isCompressed && fmt == Format.csv -> 3.6
            !isCompressed && fmt == Format.json -> 0.33
            isCompressed && fmt == Format.json -> 3.60
            isCompressed && fmt == Format.multijson -> 5.15
            !isCompressed && fmt == Format.txt -> 0.15
            isCompressed && fmt == Format.txt -> 1.8
            isCompressed && fmt == Format.psv -> 1.5
            !isCompressed && fmt == Format.parquet -> 3.35
            else -> 1.0
        }
    }
}
