package com.microsoft.azure.kusto.ingest.source;

import java.sql.ResultSet;
import java.util.UUID;

public class ResultSetSourceInfo extends SourceInfo {

    private ResultSet resultSet;

    public ResultSet getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public ResultSetSourceInfo(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public ResultSetSourceInfo(ResultSet resultSet, UUID sourceId) {
        this.resultSet = resultSet;
        this.setSourceId(sourceId);
    }

    @Override
    public String toString() {
        return String.format("ResultSet with SourceId: %s", getSourceId());
    }
}
