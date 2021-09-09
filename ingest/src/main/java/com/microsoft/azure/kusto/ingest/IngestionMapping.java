// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import java.util.Arrays;
import java.util.List;

/**
 * This class describes the ingestion mapping to use for an ingestion request.
 * When a CSV data source schema and the target schema doesn't match or when using JSON, AVRO formats,
 * there is a need to define an ingestion mapping to map the source schema to the table schema.
 * This class describes a pre-define ingestion mapping by its name- mapping reference and its kind.
 */
public class IngestionMapping {
    private ColumnMapping[] columnMappings;
    private IngestionMappingKind ingestionMappingKind;
    private String ingestionMappingReference;
    public static final List<String> mappingRequiredFormats = Arrays.asList("json", "singlejson");

    /**
     * Creates a default ingestion mapping with kind Unknown and empty mapping reference.
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
     * Please use setIngestionMappingReference for production as passing the mapping every time is wasteful
     *
     * @param columnMappings       Array of columnMappings of the same kind.
     * @param ingestionMappingKind IngestionMappingKind: the format of the source data to map from.
     */
    public IngestionMapping(ColumnMapping[] columnMappings, IngestionMappingKind ingestionMappingKind) {
        this.columnMappings = columnMappings;
        this.ingestionMappingKind = ingestionMappingKind;
    }

    /**
     * Copy constructor for IngestionMapping.
     *
     * @param other the instance to copy from
     */
    public IngestionMapping(IngestionMapping other) {
        this.ingestionMappingKind = other.ingestionMappingKind;
        this.ingestionMappingReference = other.ingestionMappingReference;
        this.columnMappings = Arrays.stream(other.columnMappings).map(ColumnMapping::new).toArray(ColumnMapping[]::new);
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
     * @param columnMappings       Array of columnMappings of the same kind.
     * @param ingestionMappingKind IngestionMappingKind: the format of the source data to map from.
     */
    public void setIngestionMapping(ColumnMapping[] columnMappings, IngestionMappingKind ingestionMappingKind) {
        this.columnMappings = columnMappings;
        this.ingestionMappingKind = ingestionMappingKind;
    }

    public IngestionMappingKind getIngestionMappingKind() {
        return ingestionMappingKind;
    }

    public String getIngestionMappingReference() {
        return ingestionMappingReference;
    }

    public ColumnMapping[] getColumnMappings() {
        return columnMappings;
    }

    /*
     Represents an ingestion mapping kind - the format of the source data to map from.
    */
    public enum IngestionMappingKind {
        unknown, Csv, Json, Parquet, Avro, ApacheAvro, Orc, W3CLogFile
    }
}
