package com.microsoft.azure.kusto.ingest.source;

import java.util.UUID;

import static com.microsoft.azure.kusto.ingest.ValidationHelper.validateIsNotBlank;


public class BlobSourceInfo extends AbstractSourceInfo {

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
        validateIsNotBlank(blobPath, "blobPath is empty");
    }
}
