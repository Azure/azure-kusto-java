package com.microsoft.azure.kusto.ingest.source;

/**
 * Transformation method can be used over all formats configured with Json mapping type (Json, Orc and Parquet)
 * See <a href="https://docs.microsoft.com/en-us/azure/kusto/management/mappings#json-mapping">kusto docs</a>
 */
public enum TransformationMethod
{
    /**
     * Comma-separated value.
     */
    None,

    /**
     * Property bag array to dictionary.
     */
    PropertyBagArrayToDictionary,

    /**
     * Source location.
     */
    SourceLocation,

    /**
     * Source line number.
     */
    SourceLineNumber,

    /**
     * Get path element.
     */
    GetPathElement,

    /**
     * Unknown method.
     */
    UnknownMethod,

    /** 
     * Converts UNIX epoch (seconds) to UTC datetime.
    */
    DateTimeFromUnixSeconds,

    /** 
     * Converts UNIX epoch (milliseconds) to UTC datetime.
     */
    DateTimeFromUnixMilliseconds,

    /** 
     * Converts UNIX epoch (microseconds) to UTC datetime.
     */
    DateTimeFromUnixMicroseconds,

    /** 
     * Converts UNIX epoch (nanoseconds) to UTC datetime.
     */
    DateTimeFromUnixNanoseconds,

}