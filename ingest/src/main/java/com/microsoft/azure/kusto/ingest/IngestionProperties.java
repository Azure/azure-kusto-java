package com.microsoft.azure.kusto.ingest;

import java.util.HashMap;
import java.util.Map;

public class IngestionProperties {
    private String databaseName;
    private String tableName;
    private boolean flushImmediately;
    private IngestionReportLevel reportLevel;
    private IngestionReportMethod reportMethod;
    private Map<String, String> additionalProperties;

    public enum DATA_FORMAT {csv, tsv, scsv, sohsv, psv, txt, json, singlejson, avro, parquet}

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean getFlushImmediately() {
        return flushImmediately;
    }

    public IngestionReportLevel getReportLevel() {
        return reportLevel;
    }

    public IngestionReportMethod getReportMethod() {
        return reportMethod;
    }

    public Map<String, String> getAdditionalProperties() {
        return additionalProperties;
    }

    public void setFlushImmediately(boolean flushImmediately) {
        this.flushImmediately = flushImmediately;
    }

    public void setReportLevel(IngestionReportLevel reportLevel) {
        this.reportLevel = reportLevel;
    }

    public void setReportMethod(IngestionReportMethod reportMethod) {
        this.reportMethod = reportMethod;
    }

    public void setDataFormat(DATA_FORMAT dataFormat) {
        additionalProperties.put("format", dataFormat.name());
    }

    /**
     * Sets the data format by its name. If the name does not exist, then it does not add it.
     *
     * @param dataFormatName One of the string values in: {@link DATA_FORMAT DATA_FORMAT}
     */
    public void setDataFormat(String dataFormatName) {
        String dataFormat = DATA_FORMAT.valueOf(dataFormatName.toLowerCase()).name();
        if (!dataFormat.isEmpty()) {
            additionalProperties.put("format", dataFormat);
        }
    }

    /**
     * Adds to the {@code additionalProperties} map, the following key-value pairs:
     * <blockquote>
     * <p>{@code jsonMappingReference} : the value of the {@code jsonMappingName} parameter
     * <p>{@code format} : {@code DATA_FORMAT.json}</p>
     * </blockquote>
     *
     * @param jsonMappingName The name of a JSON mapping declared in the destination Kusto database, that
     *                        describes the mapping between fields of a JSON object and columns of a Kusto table.
     */
    public void setJsonMappingName(String jsonMappingName) {
        additionalProperties.put("jsonMappingReference", jsonMappingName);
        additionalProperties.put("format", DATA_FORMAT.json.name());
    }

    /**
     * Adds to the {@code additionalProperties} map, the following key-value pairs:
     * <blockquote>
     * <p>{@code csvMappingReference} : the value of the {@code csvMappingName} parameter
     * <p>{@code format} : {@code DATA_FORMAT.csv}</p>
     * </blockquote>
     *
     * @param csvMappingName The name of a CSV mapping declared in the destination Kusto database, that
     *                       describes the mapping between fields of a CSV file and columns of a Kusto table.
     */
    public void setCsvMappingName(String csvMappingName) {
        additionalProperties.put("csvMappingReference", csvMappingName);
        additionalProperties.put("format", DATA_FORMAT.csv.name());
    }

    public void setAuthorizationContextToken(String token) {
        additionalProperties.put("authorizationContext", token);
    }

    /**
     * Creates an initialized {@code IngestionProperties} instance with a given {@code databaseName} and {@code tableName}.
     * The default values of the rest of the properties are:
     * <blockquote>
     * <p>{@code reportLevel} : {@code IngestionReportLevel.FailuresOnly;}</p>
     * <p>{@code reportMethod} : {@code IngestionReportMethod.Queue;}</p>
     * <p>{@code flushImmediately} : {@code false;}</p>
     * <p>{@code additionalProperties} : {@code new HashMap();}</p>
     * </blockquote>
     *
     * @param databaseName the name of the database in the destination Kusto cluster.
     * @param tableName    the name of the table in the destination database.
     */
    public IngestionProperties(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.reportLevel = IngestionReportLevel.FailuresOnly;
        this.reportMethod = IngestionReportMethod.Queue;
        this.flushImmediately = false;
        this.additionalProperties = new HashMap<>();
    }

    public enum IngestionReportLevel {
        FailuresOnly,
        None,
        FailuresAndSuccesses
    }

    public enum IngestionReportMethod {
        Queue,
        Table,
        QueueAndTable
    }

    /**
     * Validate the minimum non-empty values needed for data ingestion.
     */
    void validate() {
        Ensure.stringIsNotBlank(databaseName, "databaseName");
        Ensure.stringIsNotBlank(tableName, "tableName");
        Ensure.argIsNotNull(reportMethod, "reportMethod");
    }
}