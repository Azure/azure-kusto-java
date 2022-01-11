// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
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
    private DataFormat dataFormat;
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Creates an initialized {@code IngestionProperties} instance with a given {@code databaseName} and {@code tableName}.
     * The default values of the rest of the properties are:
     * <blockquote>
     * <p>{@code reportLevel} : {@code IngestionReportLevel.FailuresOnly;}</p>
     * <p>{@code reportMethod} : {@code IngestionReportMethod.Queue;}</p>
     * <p>{@code flushImmediately} : {@code false;}</p>
     * <p>{@code additionalProperties} : {@code new HashMap();}</p>
     * <p>{@code dataFormat} : {@code DataFormat.csv;}</p>
     * </blockquote>
     *
     * @param databaseName the name of the database in the destination Kusto cluster.
     * @param tableName    the name of the table in the destination database.
     */
    public IngestionProperties(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.reportLevel = IngestionReportLevel.FAILURES_ONLY;
        this.reportMethod = IngestionReportMethod.QUEUE;
        this.flushImmediately = false;
        this.additionalProperties = new HashMap<>();
        this.dropByTags = new ArrayList<>();
        this.ingestByTags = new ArrayList<>();
        this.ingestIfNotExists = new ArrayList<>();
        this.additionalTags = new ArrayList<>();
        this.ingestionMapping = new IngestionMapping();
        this.dataFormat = DataFormat.CSV;
    }

    /**
     * Copy constructor for {@code IngestionProperties}.
     *
     * @param other the instance to copy from.
     */
    public IngestionProperties(IngestionProperties other) {
        this.databaseName = other.databaseName;
        this.tableName = other.tableName;
        this.reportLevel = other.reportLevel;
        this.reportMethod = other.reportMethod;
        this.flushImmediately = other.flushImmediately;
        this.dataFormat = other.getDataFormat();
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
            List<String> tags = new ArrayList<>();
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
        fullAdditionalProperties.put("format", dataFormat.getKustoValue());

        String mappingReference = ingestionMapping.getIngestionMappingReference();
        if (StringUtils.isNotBlank(mappingReference)) {
            fullAdditionalProperties.put("ingestionMappingReference", mappingReference);
            fullAdditionalProperties.put("ingestionMappingType", ingestionMapping.getIngestionMappingKind().getKustoValue());
        } else if (ingestionMapping.getColumnMappings() != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
            objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

            String mapping = objectMapper.writeValueAsString(ingestionMapping.getColumnMappings());
            fullAdditionalProperties.put("ingestionMapping", mapping);
            fullAdditionalProperties.put("ingestionMappingType", ingestionMapping.getIngestionMappingKind().getKustoValue());
        }

        return fullAdditionalProperties;
    }

    /**
     * Sets the data format.
     *
     * @param dataFormat One of the values in: {@link DataFormat DataFormat}
     * @throws IllegalArgumentException if null argument is passed
     */
    public void setDataFormat(@NotNull DataFormat dataFormat) {
        Ensure.argIsNotNull(dataFormat, "dataFormat");

        this.dataFormat = dataFormat;
    }

    /**
     * Sets the data format by its name. If the name does not exist, then it does not set it.
     *
     * @param dataFormatName One of the string values in: {@link DataFormat DataFormat}
     */
    public void setDataFormat(@NotNull String dataFormatName) {
        try {
            this.dataFormat = DataFormat.valueOf(dataFormatName.toUpperCase());
        } catch (IllegalArgumentException ex) {
            log.warn("IngestionProperties.setDataFormat(): Invalid dataFormatName of {}. Per the API's specification, DataFormat property value wasn't set.", dataFormatName);
        }
    }

    /**
     * Returns the DataFormat
     *
     * @return The DataFormat
     */
    @NotNull
    public DataFormat getDataFormat() {
        return dataFormat;
    }

    /**
     * Sets the predefined ingestion mapping name:
     *
     * @param mappingReference     The name of the mapping declared in the destination Kusto database, that
     *                             describes the mapping between fields of an object and columns of a Kusto table.
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
    void validate() throws IngestionClientException {
        Ensure.stringIsNotBlank(databaseName, "databaseName");
        Ensure.stringIsNotBlank(tableName, "tableName");
        Ensure.argIsNotNull(reportMethod, "reportMethod");

        String mappingReference = ingestionMapping.getIngestionMappingReference();
        IngestionMapping.IngestionMappingKind ingestionMappingKind = ingestionMapping.getIngestionMappingKind();
        TextStringBuilder message = new TextStringBuilder();

        if ((ingestionMapping.getColumnMappings() == null) && StringUtils.isBlank(mappingReference)) {
            if (dataFormat.isMappingRequired()) {
                message.appendln("Mapping must be specified for '%s' format.", dataFormat.getKustoValue());
            }

            if (ingestionMappingKind != null) {
                message.appendln("IngestionMappingKind was defined ('%s'), so a mapping must be defined as well.", ingestionMappingKind);
            }
        } else { // a mapping was provided
            if (dataFormat.getIngestionMappingKind() != null && !dataFormat.getIngestionMappingKind().equals(ingestionMappingKind)) {
                message.appendln("Wrong ingestion mapping for format '%s'; mapping kind should be '%s', but was '%s'.",
                        dataFormat.getKustoValue(), dataFormat.getIngestionMappingKind().getKustoValue(), ingestionMappingKind != null ? ingestionMappingKind.getKustoValue() : "null");
            }

            if (ingestionMapping.getColumnMappings() != null) {
                if (StringUtils.isNotBlank(mappingReference)) {
                    message.appendln("Both mapping reference '%s' and column mappings were defined.", mappingReference);
                }

                if (ingestionMappingKind != null) {
                    for (ColumnMapping column : ingestionMapping.getColumnMappings()) {
                        if (!column.isValid(ingestionMappingKind)) {
                            message.appendln("Column mapping '%s' is invalid.", column.columnName);
                        }
                    }
                }
            }
        }

        if (!message.isEmpty()) {
            String messageStr = message.build();
            log.error(messageStr);
            throw new IngestionClientException(messageStr);
        }
    }

    public void validateResultSetProperties() throws IngestionClientException {
        Ensure.isTrue(IngestionProperties.DataFormat.CSV.equals(dataFormat),
                String.format("ResultSet translates into csv format but '%s' was given", dataFormat));

        validate();
    }

    public enum DataFormat {
        CSV("csv", IngestionMapping.IngestionMappingKind.CSV, false, true),
        TSV("tsv", IngestionMapping.IngestionMappingKind.CSV, false, true),
        SCSV("scsv", IngestionMapping.IngestionMappingKind.CSV, false, true),
        SOHSV("sohsv", IngestionMapping.IngestionMappingKind.CSV, false, true),
        PSV("psv", IngestionMapping.IngestionMappingKind.CSV, false, true),
        TXT("txt", IngestionMapping.IngestionMappingKind.CSV, false, true),
        TSVE("tsve", IngestionMapping.IngestionMappingKind.CSV, false, true),
        JSON("json", IngestionMapping.IngestionMappingKind.JSON, true, true),
        SINGLEJSON("singlejson", IngestionMapping.IngestionMappingKind.JSON, true, true),
        MULTIJSON("multijson", IngestionMapping.IngestionMappingKind.JSON, true, true),
        AVRO("avro", IngestionMapping.IngestionMappingKind.AVRO, true, false),
        APACHEAVRO("apacheavro", IngestionMapping.IngestionMappingKind.APACHEAVRO, false, true),
        PARQUET("parquet", IngestionMapping.IngestionMappingKind.PARQUET, false, false),
        SSTREAM("sstream", IngestionMapping.IngestionMappingKind.SSTREAM, false, true),
        ORC("orc", IngestionMapping.IngestionMappingKind.ORC, false, false),
        RAW("raw", IngestionMapping.IngestionMappingKind.CSV, false, true),
        W3CLOGFILE("w3clogfile", IngestionMapping.IngestionMappingKind.W3CLOGFILE, false, true);

        private final String kustoValue;
        private final IngestionMapping.IngestionMappingKind ingestionMappingKind;
        private final boolean mappingRequired;
        private final boolean compressible;

        DataFormat(String kustoValue, IngestionMapping.IngestionMappingKind ingestionMappingKind, boolean mappingRequired, boolean compressible) {
            this.kustoValue = kustoValue;
            this.ingestionMappingKind = ingestionMappingKind;
            this.mappingRequired = mappingRequired;
            this.compressible = compressible;
        }

        public String getKustoValue() {
            return kustoValue;
        }

        public IngestionMapping.IngestionMappingKind getIngestionMappingKind() {
            return ingestionMappingKind;
        }

        public boolean isMappingRequired() {
            return mappingRequired;
        }

        public boolean isCompressible() {
            return compressible;
        }
    }

    public enum IngestionReportLevel {
        FAILURES_ONLY("FailuresOnly"),
        NONE("None"),
        FAILURES_AND_SUCCESSES("FailuresAndSuccesses");

        private final String kustoValue;

        IngestionReportLevel(String kustoValue) {
            this.kustoValue = kustoValue;
        }

        public String getKustoValue() {
            return kustoValue;
        }
    }

    public enum IngestionReportMethod {
        QUEUE("Queue"),
        TABLE("Table"),
        QUEUE_AND_TABLE("QueueAndTable");

        private final String kustoValue;

        IngestionReportMethod(String kustoValue) {
            this.kustoValue = kustoValue;
        }

        public String getKustoValue() {
            return kustoValue;
        }
    }
}