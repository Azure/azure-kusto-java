package com.microsoft.azure.kusto.ingest.source;

import java.io.InputStream;
import java.util.UUID;

public class StreamSourceInfo extends SourceInfo {

    private InputStream stream;
    private boolean leaveOpen = false;

    public InputStream getStream() {
        return stream;
    }

    public void setStream(InputStream stream) {
        this.stream = stream;
    }

    public boolean isLeaveOpen() {
        return leaveOpen;
    }

    public void setLeaveOpen(boolean leaveOpen) {
        this.leaveOpen = leaveOpen;
    }

    public StreamSourceInfo(InputStream stream) {
        this.stream = stream;
    }

    public StreamSourceInfo(InputStream stream, boolean leaveOpen) {
        this.stream = stream;
        this.leaveOpen = leaveOpen;
    }

    public StreamSourceInfo(InputStream stream, boolean leaveOpen, UUID sourceId) {
        this.stream = stream;
        this.leaveOpen = leaveOpen;
        this.setSourceId(sourceId);
    }

    @Override
    public void validate(){
        if(stream == null){
            throw new IllegalArgumentException("stream is null");
        }
    }

    @Override
    public String toString() {
        return String.format("Stream with SourceId: %s", getSourceId());
    }
}

