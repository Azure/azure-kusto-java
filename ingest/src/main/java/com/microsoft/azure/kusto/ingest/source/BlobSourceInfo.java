package com.microsoft.azure.kusto.ingest.source;

import java.util.UUID;

import static com.microsoft.azure.kusto.ingest.Ensure.stringIsNotBlank;


public class BlobSourceInfo extends AbstractSourceInfo {

    private String blobPath;
    private String blobPathWithoutSecrets;


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
        this.blobPathWithoutSecrets = blobPath.split(";",2)[0].split("[?]",2)[0];
    }

    public BlobSourceInfo(String blobPath, long rawSizeInBytes) {
        this(blobPath);
        this.rawSizeInBytes = rawSizeInBytes;
    }

    public BlobSourceInfo(String blobPath, long rawSizeInBytes, UUID sourceId) {
        this(blobPath,rawSizeInBytes);
        this.setSourceId(sourceId);
    }

    public void validate() {
        stringIsNotBlank(blobPath, "blobPath");
    }

    public String getBlobPathWithoutSecrets() {
        return blobPathWithoutSecrets;
    }
}
