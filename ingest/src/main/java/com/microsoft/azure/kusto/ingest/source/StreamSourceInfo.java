package com.microsoft.azure.kusto.ingest.source;

import java.util.UUID;
import java.util.stream.Stream;

public class StreamSourceInfo extends SourceInfo {

    private Stream stream;

    public Stream getStream() {
        return stream;
    }

    public void setStream(Stream stream) {
        this.stream = stream;
    }

    public StreamSourceInfo(Stream stream) {
        this.stream = stream;
    }

    public StreamSourceInfo(Stream stream, UUID sourceId) {
        this.stream = stream;
        this.setSourceId(sourceId);
    }

    @Override
    public String toString() {
        return String.format("Stream with SourceId: %s", getSourceId());
    }
}

