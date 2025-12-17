// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.models.Format
import java.io.InputStream
import java.util.UUID

/** Represents a stream-based ingestion source. */
class StreamSource(
    stream: InputStream,
    sourceCompression: CompressionType,
    format: Format,
    sourceId: UUID = UUID.randomUUID(),
    name: String? = null,
    leaveOpen: Boolean = false,
) : LocalSource(format, leaveOpen, sourceCompression, name, sourceId) {
    init {
        mStream = stream
        initName(name)
    }

    override fun data(): InputStream {
        return mStream!!
    }

    override fun size(): Long? {
        return try {
            mStream?.available()?.toLong()
        } catch (_: Exception) {
            null
        }
    }
}
