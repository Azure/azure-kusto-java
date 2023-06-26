// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import org.jetbrains.annotations.NotNull;

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

    @Override
    public Map<String, String> getTracingAttributes() {
        return new HashMap<>();
    }

}
