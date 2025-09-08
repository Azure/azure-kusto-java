/* (C)2025 */
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
