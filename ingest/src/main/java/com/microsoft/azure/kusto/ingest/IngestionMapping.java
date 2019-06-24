package com.microsoft.azure.kusto.ingest;

public class IngestionMapping {
    public INGESTION_MAPPING_KIND IngestionMappingKind;
    public String IngestionMappingReference;

    public IngestionMapping() {
        IngestionMappingKind = INGESTION_MAPPING_KIND.unknown;
    }

    public enum INGESTION_MAPPING_KIND { unknown, csv, json, avro }
}