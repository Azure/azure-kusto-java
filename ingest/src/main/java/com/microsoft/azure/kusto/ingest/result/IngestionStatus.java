// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.models.TableEntity;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/// <summary>
/// This class represents an ingestion status.
/// </summary>
/// <remarks>
/// Any change to this class must be made in a backwards/forwards-compatible manner.
/// </remarks>
public class IngestionStatus {
    /// <summary>
    /// The updated status of the ingestion. The ingestion status will be 'Pending'
    /// during the ingestion's process
    /// and will be updated as soon as the ingestion completes.
    /// </summary>
    public OperationStatus status;
    private Map<String, Object> ingestionInfo = new HashMap<>();

    public String getStatus() {
        return status.toString();
    }

    public void setStatus(String s) {
        if (s != null) {
            setStatus(OperationStatus.valueOf(s));
        }
    }

    public void setStatus(OperationStatus st) {
        status = st;
        ingestionInfo.put("Status", st);
    }

    /// <summary>
    /// A unique identifier representing the ingested source. Can be supplied during
    /// the ingestion execution.
    /// </summary>
    public UUID ingestionSourceId;

    public UUID getIngestionSourceId() {
        return ingestionSourceId;
    }

    public void setIngestionSourceId(UUID id) {
        ingestionSourceId = id;
        ingestionInfo.put("IngestionSourceId", id);
    }

    /// <summary>
    /// The URI of the blob, potentially including the secret needed to access
    /// the blob. This can be a file system URI (on-premises deployments only),
    /// or an Azure Blob Storage URI (including a SAS key or a semicolon followed
    /// by the account key)
    /// </summary>
    public String ingestionSourcePath;

    public String getIngestionSourcePath() {
        return ingestionSourcePath;
    }

    public void setIngestionSourcePath(String path) {
        ingestionSourcePath = path;
        ingestionInfo.put("IngestionSourcePath", ingestionSourcePath);
    }

    /// <summary>
    /// The name of the database holding the target table.
    /// </summary>
    public String database;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String db) {
        database = db;
        ingestionInfo.put("Database", database);
    }

    /// <summary>
    /// The name of the target table into which the data will be ingested.
    /// </summary>
    public String table;

    public String getTable() {
        return table;
    }

    public void setTable(String t) {
        table = t;
        ingestionInfo.put("Table", table);
    }

    /// <summary>
    /// The last updated time of the ingestion status.
    /// </summary>
    public Instant updatedOn;

    public Instant getUpdatedOn() {
        return updatedOn;
    }

    public void setUpdatedOn(Instant lastUpdated) {
        updatedOn = lastUpdated;
        ingestionInfo.put("UpdatedOn", updatedOn);
    }

    /// <summary>
    /// The ingestion's operation Id.
    /// </summary>
    public UUID operationId;

    public UUID getOperationId() {
        return operationId;
    }

    public void setOperationId(UUID id) {
        operationId = id;
    }

    /// <summary>
    /// The ingestion's activity Id.
    /// </summary>
    public UUID activityId;

    public UUID getActivityId() {
        return activityId;
    }

    public void setActivityId(UUID id) {
        activityId = id;
    }

    public String errorCodeString;

    /** @deprecated This enum may be outdated compared to the error codes on the service. Use {@link #getErrorCode()} instead.
     */
    public IngestionErrorCode errorCode;

    public String getErrorCode() {
        return errorCodeString;
    }

    public void setErrorCode(String code) {
        errorCodeString = code;
        try {
            errorCode = code == null ? IngestionErrorCode.Unknown : IngestionErrorCode.valueOf(code);
        } catch (IllegalArgumentException ex) {
            errorCode = IngestionErrorCode.Misc;
        }
    }

    /// <summary>
    /// In case of a failure - indicates the failure's status.
    /// </summary>
    public IngestionFailureInfo.FailureStatusValue failureStatus;

    public String getFailureStatus() {
        return (failureStatus != null ? failureStatus : IngestionFailureInfo.FailureStatusValue.Unknown).toString();
    }

    public void setFailureStatus(String status) {
        if (status != null) {
            failureStatus = IngestionFailureInfo.FailureStatusValue.valueOf(status);
        }
    }

    /// <summary>
    /// In case of a failure - indicates the failure's details.
    /// </summary>
    public String details;

    public String getDetails() {
        return details;
    }

    public void setDetails(String d) {
        details = d;
    }

    /// <summary>
    /// In case of a failure - indicates whether or not the failures originate from
    /// an Update Policy.
    /// </summary>
    public boolean originatesFromUpdatePolicy;

    public boolean getOriginatesFromUpdatePolicy() {
        return originatesFromUpdatePolicy;
    }

    public void setOriginatesFromUpdatePolicy(boolean fromUpdatePolicy) {
        originatesFromUpdatePolicy = fromUpdatePolicy;
    }

    public IngestionStatus() {
    }

    public Map<String, Object> getEntityProperties() {
        return ingestionInfo;
    }

    public static IngestionStatus fromEntity(TableEntity tableEntity) {
        IngestionStatus ingestionStatus = new IngestionStatus();
        Object ingestionSourceId = tableEntity.getProperty("IngestionSourceId");
        ingestionStatus.setIngestionSourceId(ingestionSourceId == null ? null : (UUID) ingestionSourceId);

        ingestionStatus.setDatabase((String) tableEntity.getProperty("Database"));
        ingestionStatus.setTable((String) tableEntity.getProperty("Table"));

        Object operationId = tableEntity.getProperty("OperationId");
        ingestionStatus.setOperationId(ingestionSourceId == null ? null : (UUID) operationId);

        Object status = tableEntity.getProperty("Status");
        if (status instanceof String) {
            ingestionStatus.setStatus((String) status);
        } else {
            ingestionStatus.setStatus((OperationStatus) status);
        }

        Object activityId = tableEntity.getProperty("ActivityId");
        ingestionStatus.setActivityId(ingestionSourceId == null ? null : (UUID) activityId);

        ingestionStatus.setFailureStatus((String) tableEntity.getProperty("FailureStatus"));

        Object originatesFromUpdatePolicy = tableEntity.getProperty("OriginatesFromUpdatePolicy");
        ingestionStatus.setOriginatesFromUpdatePolicy(originatesFromUpdatePolicy != null && (boolean) originatesFromUpdatePolicy);
        ingestionStatus.setIngestionSourcePath((String) tableEntity.getProperty("IngestionSourcePath"));

        Object errorCode = tableEntity.getProperty("ErrorCode");
        if (errorCode != null) {
            ingestionStatus.setErrorCode((String) errorCode);
        }

        ingestionStatus.setDetails((String) tableEntity.getProperty("Details"));

        Object updatedOn = tableEntity.getProperty("UpdatedOn");
        if (updatedOn instanceof OffsetDateTime) {
            ingestionStatus.setUpdatedOn(((OffsetDateTime) updatedOn).toInstant());
        } else {
            ingestionStatus.setUpdatedOn((Instant) updatedOn);
        }

        return ingestionStatus;
    }
}
