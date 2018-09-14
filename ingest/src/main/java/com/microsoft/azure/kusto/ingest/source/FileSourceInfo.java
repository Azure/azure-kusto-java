package com.microsoft.azure.kusto.ingest.source;

import java.util.UUID;

public class FileSourceInfo extends SourceInfo {

    private String filePath;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    private long rawSizeInBytes;

    public long getRawSizeInBytes() {
        return rawSizeInBytes;
    }

    public void setRawSizeInBytes(long rawSizeInBytes) {
        this.rawSizeInBytes = rawSizeInBytes;
    }

    public FileSourceInfo(String filePath, long rawSizeInBytes) {
        this.filePath = filePath;
        this.rawSizeInBytes = rawSizeInBytes;
    }

    public FileSourceInfo(String filePath, long rawSizeInBytes, UUID sourceId) {
        this.filePath = filePath;
        this.rawSizeInBytes = rawSizeInBytes;
        this.setSourceId(sourceId);
    }
}
