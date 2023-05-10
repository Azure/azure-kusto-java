// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;

import java.util.Map;
import java.util.UUID;

import static com.microsoft.azure.kusto.data.Ensure.stringIsNotBlank;

public class BlobSourceInfo extends AbstractSourceInfo implements TraceableAttributes {

    private String blobPath;

    public String getBlobPath() {
        return blobPath;
    }

    public void setBlobPath(String blobPath) {
        this.blobPath = blobPath;
    }

    private long rawSizeInBytes;

    public long getRawSizeInBytes() {
        return rawSizeInBytes;
    }

    public void setRawSizeInBytes(long rawSizeInBytes) {
        this.rawSizeInBytes = rawSizeInBytes;
    }

    public BlobSourceInfo(String blobPath) {
        this.blobPath = blobPath;
    }

    public BlobSourceInfo(String blobPath, long rawSizeInBytes) {
        this.blobPath = blobPath;
        this.rawSizeInBytes = rawSizeInBytes;
    }

    public BlobSourceInfo(String blobPath, long rawSizeInBytes, UUID sourceId) {
        this.blobPath = blobPath;
        this.rawSizeInBytes = rawSizeInBytes;
        this.setSourceId(sourceId);
    }

    public void validate() {
        stringIsNotBlank(blobPath, "blobPath");
    }

    @Override
    public Map<String, String> getTracingAttributes(Map<String, String> attributes) {
        attributes.put("resource", blobPath);
        UUID sourceId = getSourceId();
        if (sourceId != null) {
            attributes.put("sourceId", sourceId.toString());
        }
        return attributes;
    }
}
