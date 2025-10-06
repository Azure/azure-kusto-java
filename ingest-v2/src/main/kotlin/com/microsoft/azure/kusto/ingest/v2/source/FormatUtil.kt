// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.models.Format

object FormatUtil {
    fun isBinaryFormat(format: Format): Boolean {
        return when (format) {
            Format.avro, Format.apacheavro, Format.parquet, Format.orc -> true
            else -> false
        }
    }
}
