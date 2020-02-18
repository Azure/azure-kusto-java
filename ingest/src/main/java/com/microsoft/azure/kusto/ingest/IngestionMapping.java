package com.microsoft.azure.kusto.ingest;

import java.util.Arrays;
import java.util.List;

/// <summary>
// This class describes the ingestion mapping to use for an ingestion request.
// When a CSV data source schema and the target schema doesn't match or when using JSON, AVRO, PARQUET or ORC formats,
// there is a need to define an ingestion mapping to map the source schema to the table schema.
// This class describes a pre-define ingestion mapping by its name- mapping reference and its kind.
/// </summary>
public class IngestionMapping {
    private ColumnMapping[] ingestionMapping;
    private IngestionMappingKind ingestionMappingKind;
    private String ingestionMappingReference;
    public final static List<String> mappingRequiredFormats = Arrays.asList("Json", "singlejson", "avro", "parquet", "orc");

    /**
     * Creates a default ingestion mapping with kind Unknown and empty mapping reference.
     */
    public IngestionMapping() {
        this.ingestionMappingKind = IngestionMappingKind.Unknown;
    }

    /**
     * Creates an ingestion mapping with the given parameters.
     *
     * @param ingestionMappingReference String: the name of the pre-defined ingestion mapping.
     * @param ingestionMappingKind      IngestionMappingKind: the format of the source data to map from.
     */
    public IngestionMapping(String ingestionMappingReference, IngestionMappingKind ingestionMappingKind) {
        this.ingestionMappingReference = ingestionMappingReference;
        this.ingestionMappingKind = ingestionMappingKind;
    }

    /**
     * Please use setIngestionMappingReference for production as passing the mapping every time is wasteful
     * @param ingestionMapping          Array of columnMappings of the same kind.
     * @param ingestionMappingKind      IngestionMappingKind: the format of the source data to map from.
     */
    public IngestionMapping(ColumnMapping[] ingestionMapping, IngestionMappingKind ingestionMappingKind) {
        this.ingestionMapping = ingestionMapping;
        this.ingestionMappingKind = ingestionMappingKind;
    }

    /**
     * Sets the ingestion mapping reference parameters
     *
     * @param ingestionMappingReference String: the name of the pre-defined ingestion mapping.
     * @param ingestionMappingKind      IngestionMappingKind: the format of the source data to map from.
     */
    public void setIngestionMappingReference(String ingestionMappingReference, IngestionMappingKind ingestionMappingKind) {
        this.ingestionMappingReference = ingestionMappingReference;
        this.ingestionMappingKind = ingestionMappingKind;
    }

    /**
     * Please use setIngestionMappingReference for production as passing the mapping every time is wasteful
     * Sets the ingestion mapping parameters
     *
     * @param ingestionMapping          Array of columnMappings of the same kind.
     * @param ingestionMappingKind      IngestionMappingKind: the format of the source data to map from.
     */
    public void setIngestionMapping(ColumnMapping[] ingestionMapping, IngestionMappingKind ingestionMappingKind) {
        this.ingestionMapping = ingestionMapping;
        this.ingestionMappingKind = ingestionMappingKind;
    }

    public IngestionMappingKind getIngestionMappingKind() {
        return ingestionMappingKind;
    }

    public String getIngestionMappingReference() {
        return ingestionMappingReference;
    }

    public ColumnMapping[] getIngestionMapping() {
        return ingestionMapping;
    }

    /// Represents an ingestion mapping kind - the format of the source data to map from.
    public enum IngestionMappingKind {
        Unknown, Csv, Json, parquet, avro, orc
    }
}
