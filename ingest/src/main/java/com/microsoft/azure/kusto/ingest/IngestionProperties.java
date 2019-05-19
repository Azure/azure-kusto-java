package com.microsoft.azure.kusto.ingest;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IngestionProperties {
    private String databaseName;
    private String tableName;
    private boolean flushImmediately;
    private IngestionReportLevel reportLevel;
    private IngestionReportMethod reportMethod;
    private ArrayList<String> dropByTags;
    private ArrayList<String> ingestByTags;
    private ArrayList<String> additionalTags;
    private ArrayList<String> ingestIfNotExists;

    private Map<String, String> additionalProperties;

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
        this.dropByTags = new ArrayList<String>();
        this.ingestByTags = new ArrayList<String>();
        this.ingestIfNotExists = new ArrayList<String>();
        this.additionalTags = new ArrayList<String>();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean getFlushImmediately() {
        return flushImmediately;
    }

    public void setFlushImmediately(boolean flushImmediately) {
        this.flushImmediately = flushImmediately;
    }

    public IngestionReportLevel getReportLevel() {
        return reportLevel;
    }

    public void setReportLevel(IngestionReportLevel reportLevel) {
        this.reportLevel = reportLevel;
    }

    public IngestionReportMethod getReportMethod() {
        return reportMethod;
    }

    public void setReportMethod(IngestionReportMethod reportMethod) {
        this.reportMethod = reportMethod;
    }

    public ArrayList<String> getDropByTags() {
        return dropByTags;
    }

    /**
     * Drop-by tags are tags added to the ingested data bulk inorder to be able to delete it.
     * This should be used with care - See <a href="https://docs.microsoft.com/en-us/azure/kusto/management/extents-overview#drop-by-extent-tags">kusto docs</a>
     *
     * @param dropByTags - suffixes tags list to tag the data being ingested, the resulted tag will be trailed by "drop-by"
     */
    public void setDropByTags(ArrayList<String> dropByTags) {
        this.dropByTags = dropByTags;
    }

    public ArrayList<String> getIngestByTags() {
        return ingestByTags;
    }

    /**
     * Tags that start with an ingest-by: prefix can be used to ensure that data is only ingested once.
     * This should be used with care - See <a href="https://docs.microsoft.com/en-us/azure/kusto/management/extents-overview#ingest-by-extent-tags">kusto docs</a>
     *
     * @param ingestByTags - suffixes tags list to tag the data being ingested, the resulted tag will be trailed by "ingest-by"
     */
    public void setIngestByTags(ArrayList<String> ingestByTags) {
        this.ingestByTags = ingestByTags;
    }

    public ArrayList<String> getAdditionalTags() {
        return additionalTags;
    }

    /**
     * Customized tags
     *
     * @param additionalTags
     */
    public void setAdditionalTags(ArrayList<String> additionalTags) {
        this.additionalTags = additionalTags;
    }

    /**
     * @param additionalProperties - Set additional properties to the ingestion properties
     */
    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public ArrayList<String> getIngestIfNotExists() {
        return ingestIfNotExists;
    }

    /**
     * Will trigger a check if there's already an extent with this specific "ingest-by" tag prefix
     * See <a href="https://docs.microsoft.com/en-us/azure/kusto/management/extents-overview#ingest-by-extent-tags">kusto docs</a>
     *
     * @param ingestIfNotExists
     */
    public void setIngestIfNotExists(ArrayList<String> ingestIfNotExists) {
        this.ingestIfNotExists = ingestIfNotExists;
    }

    Map<String, String> getIngestionProperties() throws IOException {
        Map<String, String> fullAdditionalProperties = new HashMap<>();
        if (!dropByTags.isEmpty() || !ingestByTags.isEmpty() || !additionalTags.isEmpty()) {
            ArrayList<String> tags = new ArrayList<>();
            if (!additionalTags.isEmpty()) {
                tags.addAll(additionalTags);
            }
            if (!ingestByTags.isEmpty()) {
                for (String t : ingestByTags) {
                    tags.add(String.format("%s%s", "drop-by:", t));
                }
            }
            if (!dropByTags.isEmpty()) {
                for (String t : ingestByTags) {
                    tags.add(String.format("%s%s", "ingest-by:", t));
                }
            }

            ObjectMapper objectMapper = new ObjectMapper();
            String tagsAsJson = objectMapper.writeValueAsString(tags);
            fullAdditionalProperties.put("tags", tagsAsJson);
        }

        if (!ingestIfNotExists.isEmpty()) {
            ObjectMapper objectMapper = new ObjectMapper();
            String ingestIfNotExistsJson = objectMapper.writeValueAsString(ingestIfNotExists);
            fullAdditionalProperties.put("ingestIfNotExists", ingestIfNotExistsJson);
        }
        fullAdditionalProperties.putAll(additionalProperties);
        return fullAdditionalProperties;
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
     * Validate the minimum non-empty values needed for data ingestion.
     */
    void validate() {
        Ensure.stringIsNotBlank(databaseName, "databaseName");
        Ensure.stringIsNotBlank(tableName, "tableName");
        Ensure.argIsNotNull(reportMethod, "reportMethod");
    }

    public enum DATA_FORMAT {csv, tsv, scsv, sohsv, psv, txt, json, singlejson, avro, parquet}

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
}