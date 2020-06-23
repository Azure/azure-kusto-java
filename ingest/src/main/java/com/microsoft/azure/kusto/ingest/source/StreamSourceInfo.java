// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import com.microsoft.azure.kusto.ingest.Ensure;

import java.io.InputStream;
import java.util.Objects;
import java.util.UUID;

public class StreamSourceInfo extends AbstractSourceInfo {

    private InputStream stream;
    private boolean leaveOpen = false;
    private CompressionType compressionType = null;

    public InputStream getStream() {
        return stream;
    }

    public void setStream(InputStream stream) {
        this.stream = Objects.requireNonNull(stream, "stream cannot be null");
    }

    public boolean isLeaveOpen() {
        return leaveOpen;
    }

    /**
     * Weather or not the stream will close after reading from it.
     * NOTE!!! In the streamingIngestClient - the Http client closes the stream anyway, therefore it the stream was set
     * as not compressed the stream will close regardless of the leaveOpen argument.
     */
    public void setLeaveOpen(boolean leaveOpen) {
        this.leaveOpen = leaveOpen;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public CompressionType getCompressionType() {
        return compressionType;
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
        Ensure.argIsNotNull(stream, "stream");
        Ensure.isTrue(compressionType != CompressionType.zip, "streaming ingest is not working with zip compression");
    }

    @Override
    public String toString() {
        return String.format("Stream with SourceId: %s", getSourceId());
    }
}

