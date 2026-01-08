// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.models.Format
import java.io.InputStream
import java.util.UUID

/** Represents a stream-based ingestion source. */
class StreamSource
@JvmOverloads
constructor(
    stream: InputStream,
    format: Format,
    sourceCompression: CompressionType,
    sourceId: UUID = UUID.randomUUID(),
    baseName: String? = null,
    leaveOpen: Boolean = false,
) : LocalSource(format, leaveOpen, sourceCompression, baseName, sourceId) {

    init {
        mStream = stream
        initName(baseName)
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
