// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;

import java.sql.ResultSet;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents the ResultSet source information used for ingestion.
 */
public class ResultSetSourceInfo extends AbstractSourceInfo implements TraceableAttributes {

    private ResultSet resultSet;

    /**
     * Creates a ResultSetSourceInfo.
     *
     * @param resultSet The ResultSet with the data to be ingested.
     */
    public ResultSetSourceInfo(ResultSet resultSet) {
        setResultSet(resultSet);
    }

    /**
     * Creates a ResultSetSourceInfo
     *
     * @param resultSet The ResultSet with the data to be ingested.
     * @param sourceId  An identifier that could later be used to trace this specific source data.
     */
    public ResultSetSourceInfo(ResultSet resultSet, UUID sourceId) {
        setResultSet(resultSet);
        this.setSourceId(sourceId);
    }

    /**
     * Gets the ResultSet.
     *
     * @return The ResultSet in the SourceInfo
     */
    public ResultSet getResultSet() {
        return resultSet;
    }

    /**
     * Sets the ResultSet.
     *
     * @param resultSet The ResultSet with the data to be ingested.
     */
    @SuppressWarnings("WeakerAccess")
    public void setResultSet(ResultSet resultSet) {
        this.resultSet = Objects.requireNonNull(resultSet, "ResultSet cannot be null");
    }

    @Override
    public String toString() {
        return String.format("ResultSet with SourceId: %s", getSourceId());
    }

    public void validate() {
        // nothing to validate as of now.
    }

    public Map<String, String> getTracingAttributes(Map<String, String> attributes) {
        attributes.put("resource", "resultSet");
        UUID sourceId = getSourceId();
        if (sourceId != null) {
            attributes.put("sourceId", sourceId.toString());
        }
        return attributes;
    }
}
