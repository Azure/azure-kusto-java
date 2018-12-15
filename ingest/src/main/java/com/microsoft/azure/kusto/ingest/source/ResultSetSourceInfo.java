package com.microsoft.azure.kusto.ingest.source;

import java.sql.ResultSet;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents the Resultset source information used for ingestion.
 */
public class ResultSetSourceInfo extends AbstractSourceInfo {

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
     * @param resultSet The ResultSet with the data to be ingested.
     * @param sourceId An identifier that could later be used to trace this specific source data.
     */
    public ResultSetSourceInfo(ResultSet resultSet, UUID sourceId) {
        setResultSet(resultSet);
        this.setSourceId(sourceId);
    }

    /**
     * Gets the ResultSet.
     * @return
     */
    public ResultSet getResultSet() {
        return resultSet;
    }

    /**
     * Sets the ResultSet.
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

    /**
     * Checks that this SourceInfo is defined appropriately.
     */
    public void validate() {
        //nothing to validate as of now.
    }
}
