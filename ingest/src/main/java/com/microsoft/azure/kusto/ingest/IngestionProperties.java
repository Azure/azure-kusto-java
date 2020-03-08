package com.microsoft.azure.kusto.ingest;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
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
    private IngestionMapping ingestionMapping;
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
        this.dropByTags = new ArrayList<>();
        this.ingestByTags = new ArrayList<>();
        this.ingestIfNotExists = new ArrayList<>();
        this.additionalTags = new ArrayList<>();
        this.ingestionMapping = new IngestionMapping();
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
                    tags.add(String.format("%s%s", "ingest-by:", t));
                }
            }
            if (!dropByTags.isEmpty()) {
                for (String t : dropByTags) {
                    tags.add(String.format("%s%s", "drop-by:", t));
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

        String mappingReference = ingestionMapping.getIngestionMappingReference();
        if (StringUtils.isNotBlank(mappingReference)) {
            fullAdditionalProperties.put("ingestionMappingReference", mappingReference);
            fullAdditionalProperties.put("ingestionMappingType", ingestionMapping.getIngestionMappingKind().toString());
        } else if (ingestionMapping.getIngestionMapping() != null) {
            ObjectMapper objectMapper = new ObjectMapper();

            String mapping = objectMapper.writeValueAsString(ingestionMapping.getIngestionMapping());
            fullAdditionalProperties.put("ingestionMapping", mapping);
            fullAdditionalProperties.put("ingestionMappingType", ingestionMapping.getIngestionMappingKind().toString());
        }

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

    public String getDataFormat() {
        return additionalProperties.get("format");
    }

    /**
     * Sets the predefined ingestion mapping name:
     *
     * @param mappingReference     The name of a the mapping declared in the destination Kusto database, that
     *                             describes the mapping between fields of a object and columns of a Kusto table.
     * @param ingestionMappingKind The data format of the object to map.
     */
    public void setIngestionMapping(String mappingReference, IngestionMapping.IngestionMappingKind ingestionMappingKind) {
        this.ingestionMapping.setIngestionMappingReference(mappingReference, ingestionMappingKind);
    }

    /**
     * Please use a mappingReference for production as passing the mapping every time is wasteful
     * Creates an ingestion mapping using the described column mappings:
     *
     * @param columnMappings The columnMapping used for this ingestion     .
     * @param ingestionMappingKind The data format of the object to map.
     */
    public void setIngestionMapping(ColumnMapping[] columnMappings,IngestionMapping.IngestionMappingKind ingestionMappingKind) {
        this.ingestionMapping.setIngestionMapping(columnMappings, ingestionMappingKind);
    }


    public void setIngestionMapping(IngestionMapping ingestionMapping) {
        this.ingestionMapping = ingestionMapping;
    }

    public IngestionMapping getIngestionMapping() {
        return this.ingestionMapping;
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
        Ensure.argIsNotNull(StringUtils.isNotBlank(ingestionMapping.getIngestionMappingReference())
                && ingestionMapping.getIngestionMapping() != null, "ingestionMapping");
    }

    public enum DATA_FORMAT {csv, tsv, scsv, sohsv, psv, txt, tsve, json, singlejson, multijson, avro, parquet, orc}

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