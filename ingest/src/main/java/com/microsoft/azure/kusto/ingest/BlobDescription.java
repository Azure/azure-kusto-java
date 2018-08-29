package com.microsoft.azure.kusto.ingest;

import java.util.UUID;

public class BlobDescription {
    private String blobPath;
    private Long blobSize;
    private UUID sourceId;

    public String getBlobPath()
    {
        return blobPath;
    }

    public void setBlobPath(String blobPath)
    {
        this.blobPath = blobPath;
    }

    public Long getBlobSize()
    {
        return blobSize;
    }

    public void setBlobSize(Long blobSize)
    {
        this.blobSize = blobSize;
    }

    public UUID getSourceId()
    {
        return sourceId;
    }

    public void setSourceId(UUID sourceId)
    {
        this.sourceId = sourceId;
    }

    public BlobDescription()
    {
    }

    public BlobDescription(String blobPath, Long blobSize)
    {
        this.blobPath = blobPath;
        this.blobSize = blobSize;
    }
}
