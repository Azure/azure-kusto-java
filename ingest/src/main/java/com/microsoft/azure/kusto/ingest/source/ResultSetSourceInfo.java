package com.microsoft.azure.kusto.ingest.source;

import java.sql.ResultSet;
import java.util.Objects;
import java.util.UUID;

public class ResultSetSourceInfo extends AbstractSourceInfo {

    private ResultSet resultSet;

    public ResultSetSourceInfo(ResultSet resultSet) {
        setResultSet(resultSet);
    }

    public ResultSetSourceInfo(ResultSet resultSet, UUID sourceId) {
        setResultSet(resultSet);
        this.setSourceId(sourceId);
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    @SuppressWarnings("WeakerAccess")
    public void setResultSet(ResultSet resultSet) {
        this.resultSet = Objects.requireNonNull(resultSet, "ResultSet cannot be null");
    }

    @Override
    public String toString() {
        return String.format("ResultSet with SourceId: %s", getSourceId());
    }

    public void validate() {
        //nothing to validate as of now.
    }
}
