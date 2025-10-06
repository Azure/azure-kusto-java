// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.source

import com.microsoft.azure.kusto.ingest.v2.models.Format

class BlobSource : IngestionSource {
    override val url: String
    val exactSize: Int?

    constructor(
        url: String,
        format: Format,
        compression: CompressionType? = null,
        sourceId: String? = null,
    ) : super(
        format,
        compression ?: ExtendedDataSourceCompressionType.detectFromUri(url),
        url,
        sourceId,
    ) {
        this.url = url
        this.exactSize = null
    }

    internal constructor(
        url: String,
        localSource: LocalSource,
        exactSize: Int? = null,
    ) : super(
        localSource.format,
        localSource.compressionType,
        url,
        localSource.sourceId,
    ) {
        this.url = url
        this.exactSize = exactSize
    }

    override fun toString(): String {
        // Assuming FormatWithInvariantCulture is replaced by Kotlin string interpolation
        return "$url SourceId: $sourceId"
    }

    // No resources to close; method intentionally left empty.
    override fun close() {
        TODO("Not yet implemented")
    }
}
