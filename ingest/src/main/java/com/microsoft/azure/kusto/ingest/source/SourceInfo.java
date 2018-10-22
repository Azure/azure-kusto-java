package com.microsoft.azure.kusto.ingest.source;

import java.util.UUID;

abstract class SourceInfo  implements ISourceInfo {

    private UUID sourceId;

    public UUID getSourceId() {
        return sourceId;
    }

    public void setSourceId(UUID sourceId) {
        this.sourceId = sourceId;
    }

}
