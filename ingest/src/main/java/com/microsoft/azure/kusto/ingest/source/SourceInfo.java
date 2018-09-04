package com.microsoft.azure.kusto.ingest.source;

import java.util.UUID;

public abstract class SourceInfo {

    private UUID sourceId;

    public UUID getSourceId() {
        return sourceId;
    }

    public void setSourceId(UUID sourceId) {
        this.sourceId = sourceId;
    }

}
