// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import java.util.Map;
import java.util.UUID;

import static com.microsoft.azure.kusto.data.Ensure.stringIsNotBlank;

public class FileSourceInfo extends AbstractSourceInfo {

    private String filePath;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public FileSourceInfo(String filePath, long rawSizeInBytes) {
        this.filePath = filePath;
        this.setRawSizeInBytes(rawSizeInBytes);
    }

    public FileSourceInfo(String filePath, long rawSizeInBytes, UUID sourceId) {
        this(filePath, rawSizeInBytes);
        this.setSourceId(sourceId);
    }

    public void validate() {
        stringIsNotBlank(filePath, "filePath");
    }

    public Map<String, String> getTracingAttributes() {
        Map<String, String> attributes = super.getTracingAttributes();
        attributes.put("resource", filePath);
        UUID sourceId = getSourceId();
        if (sourceId != null) {
            attributes.put("sourceId", sourceId.toString());
        }
        return attributes;
    }
}
