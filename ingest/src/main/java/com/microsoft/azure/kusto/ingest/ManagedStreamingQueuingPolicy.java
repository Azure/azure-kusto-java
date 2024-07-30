package com.microsoft.azure.kusto.ingest;

interface ManagedStreamingQueuingPolicyPredicator {
    boolean shouldUseQueuedIngestion(long dataSize, long rawDataSize, boolean compressed, IngestionProperties.DataFormat dataFormat);
}

public class ManagedStreamingQueuingPolicy implements ManagedStreamingQueuingPolicyPredicator {
    static final int MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES = 4 * 1024 * 1024;
    // Regardless of the format, we don't want to stream more than 10mb
    static final int MAX_STREAMING_STREAM_SIZE_BYTES = 10 * 1024 * 1024;
    // Used against the users input of raw data size
    static final int MAX_STREAMING_RAW_SIZE_BYTES = 6 * 1024 * 1024;
    static final double JSON_UNCOMPRESSED_FACTOR = 1.5d;
    static final int NON_BINARY_FACTOR = 2;
    static final double BINARY_COMPRESSED_FACTOR = 2d;
    static final double BINARY_UNCOMPRESSED_FACTOR = 1.5d;

    public ManagedStreamingQueuingPolicy() {
    }

    // Return true if streaming ingestion should not be tried, according to stream size, compression and format
    public boolean shouldUseQueuedIngestion(long dataSize, long rawDataSize, boolean compressed, IngestionProperties.DataFormat dataFormat) {
        // if size is given - use the 6mb limit.
        if (rawDataSize > 0) {
            return rawDataSize > MAX_STREAMING_RAW_SIZE_BYTES;
        }

        // In case available() was implemented wrong, do streaming
        if (dataSize <= 0) {
            return false;
        }

        // In any case - don't stream more than 10mb
        if (dataSize > MAX_STREAMING_STREAM_SIZE_BYTES) {
            return true;
        }

        if (!dataFormat.isCompressible()) {
            // Binary format
            if (compressed) {
                return (dataSize * BINARY_COMPRESSED_FACTOR) > MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES;
            }

            return (dataSize * BINARY_UNCOMPRESSED_FACTOR) > MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES;
        }

        if (compressed) {
            // Compressed + non-binary
            return (dataSize * NON_BINARY_FACTOR) > MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES;
        }

        if (dataFormat.isJsonFormat()) {
            // JSON uncompressed format
            return (dataSize / JSON_UNCOMPRESSED_FACTOR) > MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES;
        }

        // Uncompressed + non-binary
        return (dataSize / NON_BINARY_FACTOR) > MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES;
    }

    static ManagedStreamingQueuingPolicy Default = new ManagedStreamingQueuingPolicy();
}
