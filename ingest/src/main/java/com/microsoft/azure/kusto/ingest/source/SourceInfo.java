package com.microsoft.azure.kusto.ingest.source;

import java.util.UUID;

public interface SourceInfo {
    void validate();
    UUID getSourceId();
    void setSourceId(UUID sourceId);
}
