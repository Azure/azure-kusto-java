// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import java.util.Map;
import java.util.UUID;

import static com.microsoft.azure.kusto.data.Ensure.stringIsNotBlank;

public class BlobSourceInfo extends AbstractSourceInfo {

    private String blobPath;
    private CompressionType compressionType;

    public String getBlobPath() {
        return blobPath;
    }

    public void setBlobPath(String blobPath) {
        this.blobPath = blobPath;
    }

    public BlobSourceInfo(String blobPath) {
        this.blobPath = blobPath;
    }

    public BlobSourceInfo(String blobPath, long rawSizeInBytes) {
        this(blobPath);
        this.setRawSizeInBytes(rawSizeInBytes);
    }

    public BlobSourceInfo(String blobPath, long rawSizeInBytes, CompressionType compressionType) {
        this(blobPath);
        this.compressionType = compressionType;
        this.setRawSizeInBytes(rawSizeInBytes);
    }

    public BlobSourceInfo(String blobPath, long rawSizeInBytes, UUID sourceId) {
        this(blobPath, rawSizeInBytes);
        this.setSourceId(sourceId);
    }


    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }


    public void validate() {
        stringIsNotBlank(blobPath, "blobPath");
    }

    @Override
    public Map<String, String> getTracingAttributes() {
        Map<String, String> attributes = super.getTracingAttributes();
        attributes.put("resource", blobPath);
        UUID sourceId = getSourceId();
        if (sourceId != null) {
            attributes.put("sourceId", sourceId.toString());
        }
        return attributes;
    }
}
