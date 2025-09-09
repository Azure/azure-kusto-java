// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

enum class CompressionType {
    GZIP,
    ZIP,
    NONE,
    ;

    override fun toString(): String {
        return if (this == NONE) "" else name
    }
}
