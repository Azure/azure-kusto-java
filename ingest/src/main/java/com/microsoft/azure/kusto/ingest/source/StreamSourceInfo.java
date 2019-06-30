package com.microsoft.azure.kusto.ingest.source;

import com.microsoft.azure.kusto.ingest.Ensure;

import java.io.InputStream;
import java.util.Objects;
import java.util.UUID;

public class StreamSourceInfo extends AbstractSourceInfo {

    private InputStream stream;
    private boolean leaveOpen = false;
    private boolean isCompressed = false;

    public InputStream getStream() {
        return stream;
    }

    public void setStream(InputStream stream) {
        this.stream = Objects.requireNonNull(stream, "stream cannot be null");
    }

    public boolean isLeaveOpen() {
        return leaveOpen;
    }

    public void setLeaveOpen(boolean leaveOpen) {
        this.leaveOpen = leaveOpen;
    }

    public void setIsCompressed(boolean isCompressed) {
        this.isCompressed = isCompressed;
    }

    public boolean getIsCompressed() {
        return isCompressed;
    }

    public StreamSourceInfo(InputStream stream) {
        setStream(stream);
    }

    public StreamSourceInfo(InputStream stream, boolean leaveOpen) {
        setLeaveOpen(leaveOpen);
        setStream(stream);
    }

    public StreamSourceInfo(InputStream stream, boolean leaveOpen, UUID sourceId) {
        setLeaveOpen(leaveOpen);
        setStream(stream);
        setSourceId(sourceId);
    }

    public void validate() {
        // nothing to validate as of now.
    }

    @Override
    public String toString() {
        return String.format("Stream with SourceId: %s", getSourceId());
    }
}
