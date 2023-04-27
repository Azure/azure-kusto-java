// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;

import java.util.Map;
import java.util.UUID;

import static com.microsoft.azure.kusto.data.Ensure.stringIsNotBlank;

public class FileSourceInfo extends AbstractSourceInfo implements TraceableAttributes {

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

    public void validate() {
        stringIsNotBlank(filePath, "filePath");
    }

    public Map<String, String> addTraceAttributes(Map<String, String> attributes) {
        attributes.put("resource", filePath);
        UUID sourceId = getSourceId();
        if (sourceId != null) {
            attributes.put("sourceId", sourceId.toString());
        }
        return attributes;
    }
}
