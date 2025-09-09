/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2.source

enum class DataFormat(
    val kustoValue: String,
    private val ingestionMappingKind: IngestionMappingKind,
    compressible: Boolean,
) {
    CSV("csv", IngestionMappingKind.CSV, true),
    TSV("tsv", IngestionMappingKind.CSV, true),
    SCSV("scsv", IngestionMappingKind.CSV, true),
    SOHSV("sohsv", IngestionMappingKind.CSV, true),
    PSV("psv", IngestionMappingKind.CSV, true),
    TXT("txt", IngestionMappingKind.CSV, true),
    TSVE("tsve", IngestionMappingKind.CSV, true),
    JSON("json", IngestionMappingKind.JSON, true),
    SINGLEJSON("singlejson", IngestionMappingKind.JSON, true),
    MULTIJSON("multijson", IngestionMappingKind.JSON, true),
    AVRO("avro", IngestionMappingKind.AVRO, false),
    APACHEAVRO("apacheavro", IngestionMappingKind.APACHEAVRO, false),
    PARQUET("parquet", IngestionMappingKind.PARQUET, false),
    SSTREAM("sstream", IngestionMappingKind.SSTREAM, false),
    ORC("orc", IngestionMappingKind.ORC, false),
    RAW("raw", IngestionMappingKind.CSV, true),
    W3CLOGFILE("w3clogfile", IngestionMappingKind.W3CLOGFILE, true),
    ;

    val isCompressible: Boolean = compressible

    fun getIngestionMappingKind(): IngestionMappingKind {
        return ingestionMappingKind
    }

    fun isBinaryFormat(): Boolean {
        return this == AVRO ||
            this == APACHEAVRO ||
            this == PARQUET ||
            this == SSTREAM ||
            this == ORC
    }

    fun isJsonFormat(): Boolean {
        return this == JSON || this == MULTIJSON || this == SINGLEJSON
    }

    fun toKustoValue(): String {
        return kustoValue
    }
}

enum class IngestionMappingKind(val kustoValue: String) {
    CSV("Csv"),
    JSON("Json"),
    AVRO("Avro"),
    PARQUET("Parquet"),
    SSTREAM("SStream"),
    ORC("Orc"),
    APACHEAVRO("ApacheAvro"),
    W3CLOGFILE("W3CLogFile"),
}
