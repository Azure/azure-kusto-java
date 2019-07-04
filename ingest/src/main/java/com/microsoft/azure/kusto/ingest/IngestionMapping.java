package com.microsoft.azure.kusto.ingest;

public class IngestionMapping {
    private IngestionMappingKind ingestionMappingKind;
    private String ingestionMappingReference;

    public IngestionMapping() {
        this.ingestionMappingKind = IngestionMappingKind.unknown;
    }

    public IngestionMapping(String ingestionMappingReference, IngestionMappingKind ingestionMappingKind) {
        this.ingestionMappingReference = ingestionMappingReference;
        this.ingestionMappingKind = ingestionMappingKind;
    }

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

    public enum IngestionMappingKind {unknown, csv, json, avro}
}