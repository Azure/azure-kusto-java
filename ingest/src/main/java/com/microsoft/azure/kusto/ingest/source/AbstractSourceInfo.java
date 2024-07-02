// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

abstract class AbstractSourceInfo implements SourceInfo, TraceableAttributes {
    private UUID sourceId;

    public UUID getSourceId() {
        return sourceId;
    }

    public void setSourceId(UUID sourceId) {
        this.sourceId = sourceId;
    }

    // An estimation of the raw (uncompressed, un-indexed) size of the data, for binary formatted files - use only if known
    private long rawSizeInBytes;

    public long getRawSizeInBytes() {
        return rawSizeInBytes;
    }

    // An estimation of the raw (uncompressed, un-indexed) size of the data, for binary formatted files - use only if known
    public void setRawSizeInBytes(long rawSizeInBytes) {
        this.rawSizeInBytes = rawSizeInBytes;
    }

    @Override
    public Map<String, String> getTracingAttributes() {
        return new HashMap<>();
    }
}
