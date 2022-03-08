// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.microsoft.azure.kusto.ingest.result.IngestionStatusInTableDescription;
import com.microsoft.azure.kusto.ingest.result.ValidationPolicy;
import java.util.Map;
import java.util.UUID;

public final class IngestionBlobInfo {
    private final String blobPath;
    private Long rawDataSize;
    private final String databaseName;
    private final String tableName;
    private UUID id;
    private final Boolean retainBlobOnSuccess;
    private String reportLevel;
    private String reportMethod;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private ValidationPolicy validationPolicy;

    private Boolean flushImmediately;
    private IngestionStatusInTableDescription ingestionStatusInTable;
    private Map<String, String> additionalProperties;

    public IngestionBlobInfo(String blobPath, String databaseName, String tableName) {
        this.blobPath = blobPath;
        this.databaseName = databaseName;
        this.tableName = tableName;
        id = UUID.randomUUID();
        retainBlobOnSuccess = true;
        reportLevel = IngestionProperties.IngestionReportLevel.FAILURES_ONLY.getKustoValue();
        reportMethod = IngestionProperties.IngestionReportMethod.QUEUE.getKustoValue();
        flushImmediately = false;
    }

    public String getBlobPath() {
        return blobPath;
    }

    public Long getRawDataSize() {
        return rawDataSize;
    }

    public void setRawDataSize(Long rawDataSize) {
        this.rawDataSize = rawDataSize;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Boolean getRetainBlobOnSuccess() {
        return retainBlobOnSuccess;
    }

    public String getReportLevel() {
        return reportLevel;
    }

    public void setReportLevel(String reportLevel) {
        this.reportLevel = reportLevel;
    }

    public String getReportMethod() {
        return reportMethod;
    }

    public void setReportMethod(String reportMethod) {
        this.reportMethod = reportMethod;
    }

    public Boolean getFlushImmediately() {
        return flushImmediately;
    }

    public void setFlushImmediately(boolean flushImmediately) {
        this.flushImmediately = flushImmediately;
    }

    public IngestionStatusInTableDescription getIngestionStatusInTable() {
        return ingestionStatusInTable;
    }

    public void setIngestionStatusInTable(IngestionStatusInTableDescription ingestionStatusInTable) {
        this.ingestionStatusInTable = ingestionStatusInTable;
    }

    public Map<String, String> getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public ValidationPolicy getValidationPolicy() {
        return validationPolicy;
    }

    public void setValidationPolicy(ValidationPolicy validationPolicy) {
        this.validationPolicy = validationPolicy;
    }
}