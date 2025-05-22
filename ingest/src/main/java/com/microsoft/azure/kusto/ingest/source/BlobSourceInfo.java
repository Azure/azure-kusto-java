// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import java.io.File;
import java.util.Map;
import java.util.UUID;

import static com.microsoft.azure.kusto.data.Ensure.stringIsNotBlank;

public class BlobSourceInfo extends AbstractSourceInfo {
    private String blobPath;

    // For internal usage - only when we create the blob
    private Long blobExactSize = null;
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

    public BlobSourceInfo(String blobPath, CompressionType compressionType) {
        this(blobPath, compressionType, null);
    }

    public BlobSourceInfo(String blobPath, CompressionType compressionType, UUID sourceId) {
        setBlobPath(blobPath);
        setCompressionType(compressionType);
        setSourceId(sourceId);
    }

    public Long getBlobExactSize() {
        return blobExactSize;
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

    /*
     * For internal usage, adding blobExactSize
     */
    public static BlobSourceInfo fromFile(String blobPath, FileSourceInfo fileSourceInfo, CompressionType sourceCompressionType, boolean gotCompressed) {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, gotCompressed ? CompressionType.gz : sourceCompressionType,
                fileSourceInfo.getSourceId());
        if (sourceCompressionType == null) {
            blobSourceInfo.blobExactSize = new File(fileSourceInfo.getFilePath()).length();
        }

        return blobSourceInfo;
    }

    /*
     * For internal usage, adding blobExactSize
     */
    public static BlobSourceInfo fromStream(String blobPath, Integer size, StreamSourceInfo streamSourceInfo) {
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobPath, streamSourceInfo.getCompressionType(), streamSourceInfo.getSourceId());
        blobSourceInfo.blobExactSize = size.longValue();
        return blobSourceInfo;
    }
}
