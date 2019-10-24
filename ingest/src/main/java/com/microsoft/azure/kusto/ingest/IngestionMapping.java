package com.microsoft.azure.kusto.ingest;

import java.util.Arrays;
import java.util.List;

/// <summary>
// This class describes the ingestion mapping to use for an ingestion request.
// When a CSV data source schema and the target schema doesn't match or when using JSON, AVRO or PARQUET formats,
// there is a need to define an ingestion mapping to map the source schema to the table schema.
// This class describes a pre-define ingestion mapping by its name- mapping reference and its kind.
/// </summary>
public class IngestionMapping {
    private IngestionMappingKind ingestionMappingKind;
    private String ingestionMappingReference;
    public final static List<String> mappingRequiredFormats = Arrays.asList("json", "singlejson", "avro", "parquet");

    /**
     * Creates a default ingestion mapping with kind unknown and empty mapping reference.
     */
    public IngestionMapping() {
        this.ingestionMappingKind = IngestionMappingKind.unknown;
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
     * Sets the ingestion mapping parameters
     *
     * @param ingestionMappingReference String: the name of the pre-defined ingestion mapping.
     * @param ingestionMappingKind      IngestionMappingKind: the format of the source data to map from.
     */
    public void setIngestionMapping(String ingestionMappingReference, IngestionMappingKind ingestionMappingKind) {
        this.ingestionMappingReference = ingestionMappingReference;
        this.ingestionMappingKind = ingestionMappingKind;
    }

    public IngestionMappingKind getIngestionMappingKind() {
        return ingestionMappingKind;
    }

    public String getIngestionMappingReference() {
        return ingestionMappingReference;
    }

    /// Represents an ingestion mapping kind - the format of the source data to map from.
    public enum IngestionMappingKind {unknown, csv, json, parquet, avro}
}