// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import org.jetbrains.annotations.NotNull;

import java.io.InputStream;
import java.util.Map;
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
     * @param leaveOpen leave the stream open after processing
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
        this(stream);
        setLeaveOpen(leaveOpen);
    }

    public StreamSourceInfo(InputStream stream, boolean leaveOpen, UUID sourceId) {
        this(stream, leaveOpen);
        setSourceId(sourceId);
    }

    public StreamSourceInfo(InputStream stream, boolean leaveOpen, UUID sourceId, CompressionType compressionType) {
        this(stream, leaveOpen, sourceId);
        setCompressionType(compressionType);
    }

    public StreamSourceInfo(InputStream stream, boolean leaveOpen, UUID sourceId, CompressionType compressionType, long size) {
        this(stream, leaveOpen, sourceId, compressionType);
        setRawSizeInBytes(size);
    }

    public void validate() {
        Ensure.argIsNotNull(stream, "stream");
        Ensure.isTrue(compressionType != CompressionType.zip, "streaming ingest is not working with zip compression");
    }

    @Override
    public String toString() {
        return String.format("Stream with SourceId: %s", getSourceId());
    }

    public Map<String, String> getTracingAttributes() {
        Map<String, String> attributes = super.getTracingAttributes();
        attributes.put("resource", "stream");
        UUID sourceId = getSourceId();
        if (sourceId != null) {
            attributes.put("sourceId", sourceId.toString());
        }
        return attributes;
    }
}
