// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Ensure;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IngestionProperties {
    private final String databaseName;
    private final String tableName;
    private boolean flushImmediately;
    private IngestionReportLevel reportLevel;
    private IngestionReportMethod reportMethod;
    private List<String> dropByTags;
    private List<String> ingestByTags;
    private List<String> additionalTags;
    private List<String> ingestIfNotExists;
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

    /**
     * Copy constructor for {@code IngestionProperties}.
     * @param other the instance to copy from.
     */
    public IngestionProperties(IngestionProperties other) {
        this.databaseName = other.databaseName;
        this.tableName = other.tableName;
        this.reportLevel = other.reportLevel;
        this.reportMethod = other.reportMethod;
        this.flushImmediately = other.flushImmediately;
        this.additionalProperties = new HashMap<>(other.additionalProperties);
        this.dropByTags = new ArrayList<>(other.dropByTags);
        this.ingestByTags = new ArrayList<>(other.ingestByTags);
        this.ingestIfNotExists = new ArrayList<>(other.ingestIfNotExists);
        this.additionalTags = new ArrayList<>(other.additionalTags);
        this.ingestionMapping = new IngestionMapping(other.ingestionMapping);
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

    public List<String> getDropByTags() {
        return dropByTags;
    }

    /**
     * Drop-by tags are tags added to the ingested data bulk inorder to be able to delete it.
     * This should be used with care - See <a href="https://docs.microsoft.com/en-us/azure/kusto/management/extents-overview#drop-by-extent-tags">kusto docs</a>
     *
     * @param dropByTags - suffixes tags list to tag the data being ingested, the resulted tag will be trailed by "drop-by"
     */
    public void setDropByTags(List<String> dropByTags) {
        this.dropByTags = dropByTags;
    }

    public List<String> getIngestByTags() {
        return ingestByTags;
    }

    /**
     * Tags that start with an ingest-by: prefix can be used to ensure that data is only ingested once.
     * This should be used with care - See <a href="https://docs.microsoft.com/en-us/azure/kusto/management/extents-overview#ingest-by-extent-tags">kusto docs</a>
     *
     * @param ingestByTags - suffixes tags list to tag the data being ingested, the resulted tag will be trailed by "ingest-by"
     */
    public void setIngestByTags(List<String> ingestByTags) {
        this.ingestByTags = ingestByTags;
    }

    public List<String> getAdditionalTags() {
        return additionalTags;
    }

    /**
     * Customized tags
     *
     * @param additionalTags list of custom user tags
     */
    public void setAdditionalTags(List<String> additionalTags) {
        this.additionalTags = additionalTags;
    }

    /**
     * @param additionalProperties - Set additional properties to the ingestion properties
     */
    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public Map<String, String> getAdditionalProperties() {
        return this.additionalProperties;
    }

    public List<String> getIngestIfNotExists() {
        return ingestIfNotExists;
    }

    /**
     * Will trigger a check if there's already an extent with this specific "ingest-by" tag prefix
     * See <a href="https://docs.microsoft.com/en-us/azure/kusto/management/extents-overview#ingest-by-extent-tags">kusto docs</a>
     *
     * @param ingestIfNotExists list of ingestIfNotExists tags
     */
    public void setIngestIfNotExists(List<String> ingestIfNotExists) {
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
        } else if (ingestionMapping.getColumnMappings() != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
            objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

            String mapping = objectMapper.writeValueAsString(ingestionMapping.getColumnMappings());
            fullAdditionalProperties.put("ingestionMapping", mapping);
            fullAdditionalProperties.put("ingestionMappingType", ingestionMapping.getIngestionMappingKind().toString());
        }

        return fullAdditionalProperties;
    }

    public void setDataFormat(DataFormat dataFormat) {
        additionalProperties.put("format", dataFormat.name());
    }

    /**
     * Sets the data format by its name. If the name does not exist, then it does not add it.
     *
     * @param dataFormatName One of the string values in: {@link DataFormat DataFormat}
     */
    public void setDataFormat(String dataFormatName) {
        String dataFormat = DataFormat.valueOf(dataFormatName.toLowerCase()).name();
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
     * @param mappingReference     The name of the mapping declared in the destination Kusto database, that
     *                             describes the mapping between fields of a object and columns of a Kusto table.
     * @param ingestionMappingKind The data format of the object to map.
     */
    public void setIngestionMapping(String mappingReference, IngestionMapping.IngestionMappingKind ingestionMappingKind) {
        this.ingestionMapping = new IngestionMapping(mappingReference, ingestionMappingKind);
    }

    /**
     * Please use a mappingReference for production as passing the mapping every time is wasteful
     * Creates an ingestion mapping using the described column mappings:
     *
     * @param columnMappings       The columnMapping used for this ingestion.
     * @param ingestionMappingKind The data format of the object to map.
     */
    public void setIngestionMapping(ColumnMapping[] columnMappings, IngestionMapping.IngestionMappingKind ingestionMappingKind) {
        this.ingestionMapping = new IngestionMapping(columnMappings, ingestionMappingKind);
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
     * Validate the minimum non-empty values needed for data ingestion and mappings.
     */
    void validate() {
        Ensure.stringIsNotBlank(databaseName, "databaseName");
        Ensure.stringIsNotBlank(tableName, "tableName");
        Ensure.argIsNotNull(reportMethod, "reportMethod");

        if (ingestionMapping.getColumnMappings() != null) {
            Ensure.isFalse(StringUtils.isNotBlank(ingestionMapping.getIngestionMappingReference()), "Both mapping reference and column mappings were defined");
            IngestionMapping.IngestionMappingKind ingestionMappingKind = ingestionMapping.getIngestionMappingKind();
            for (ColumnMapping column : ingestionMapping.getColumnMappings()) {
                Ensure.isTrue(column.isValid(ingestionMappingKind), String.format("Column mapping '%s' is invalid", column.columnName));
            }
        }
    }

    public void validateResultSetProperties() {
        String givenDataFormat = this.getDataFormat();
        if (givenDataFormat == null) {
            this.setDataFormat(IngestionProperties.DataFormat.csv);
        } else {
            Ensure.isTrue(givenDataFormat.equals(IngestionProperties.DataFormat.csv.name()),
                    String.format("ResultSet translates into csv format but '%s' was given", givenDataFormat));
        }
        validate();
    }

    public enum DataFormat {
        csv(IngestionMapping.IngestionMappingKind.Csv, false),
        tsv(IngestionMapping.IngestionMappingKind.Csv, false),
        scsv(IngestionMapping.IngestionMappingKind.Csv, false),
        sohsv(IngestionMapping.IngestionMappingKind.Csv, false),
        psv(IngestionMapping.IngestionMappingKind.Csv, false),
        txt(IngestionMapping.IngestionMappingKind.unknown, false),
        tsve(IngestionMapping.IngestionMappingKind.Csv, false),
        json(IngestionMapping.IngestionMappingKind.Json, true),
        singlejson(IngestionMapping.IngestionMappingKind.Json, true),
        multijson(IngestionMapping.IngestionMappingKind.Json, true),
        avro(IngestionMapping.IngestionMappingKind.Avro, true),
        apacheavro(IngestionMapping.IngestionMappingKind.ApacheAvro, false),
        parquet(IngestionMapping.IngestionMappingKind.Parquet, false),
        sstream(IngestionMapping.IngestionMappingKind.SStream, false),
        orc(IngestionMapping.IngestionMappingKind.Orc, false),
        raw(IngestionMapping.IngestionMappingKind.unknown, false),
        w3clogfile(IngestionMapping.IngestionMappingKind.W3CLogFile, false);

        private final IngestionMapping.IngestionMappingKind ingestionMappingKind;
        private final boolean mappingRequired;

        DataFormat(IngestionMapping.IngestionMappingKind ingestionMappingKind, boolean mappingRequired) {
            this.ingestionMappingKind = ingestionMappingKind;
            this.mappingRequired = mappingRequired;
        }

        public IngestionMapping.IngestionMappingKind getIngestionMappingKind() {
            return ingestionMappingKind;
        }

        public boolean isMappingRequired() {
            return mappingRequired;
        }
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
}
