package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Ensure;

interface ManagedStreamingQueuingPolicyPredicator {
    boolean shouldUseQueuedIngestion(long dataSize, boolean compressed, IngestionProperties.DataFormat dataFormat);
}

public class ManagedStreamingQueuingPolicy implements ManagedStreamingQueuingPolicyPredicator {
    static final int MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES = 4 * 1024 * 1024;
    // Regardless of the format, we don't want to stream more than 10mb
    static final int MAX_STREAMING_STREAM_SIZE_BYTES = 10 * 1024 * 1024;

    static final double JSON_UNCOMPRESSED_FACTOR = 1.5d;
    static final int NON_BINARY_FACTOR = 2;
    static final double BINARY_COMPRESSED_FACTOR = 2d;
    static final double BINARY_UNCOMPRESSED_FACTOR = 1.5d;
    private final double factor;

    public ManagedStreamingQueuingPolicy(double factor) {
        Ensure.isTrue(factor > 0, "ManagedStreamingQueuingPolicy: factor should be greater than 0");
        this.factor = factor;
    }

    // Return true if streaming ingestion should not be tried, according to stream size, compression and format
    public boolean shouldUseQueuedIngestion(long dataSize, boolean compressed, IngestionProperties.DataFormat dataFormat) {
        // In case available() was implemented wrong, do streaming
        if (dataSize <= 0) {
            return false;
        }

        // In any case - don't stream more than 10mb
        if (dataSize > factor * MAX_STREAMING_STREAM_SIZE_BYTES) {
            return true;
        }

        if (!dataFormat.isCompressible()) {
            // Binary format
            if (compressed) {
                return (dataSize * BINARY_COMPRESSED_FACTOR) > factor * MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES;
            }

            return (dataSize * BINARY_UNCOMPRESSED_FACTOR) > factor * MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES;
        }

        if (compressed) {
            // Compressed + non-binary
            return (dataSize * NON_BINARY_FACTOR) > factor * MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES;
        }

        if (dataFormat.isJsonFormat()) {
            // JSON uncompressed format
            return (dataSize / JSON_UNCOMPRESSED_FACTOR) > factor * MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES;
        }

        // Uncompressed + non-binary
        return ((double) dataSize / NON_BINARY_FACTOR) > factor * MAX_STREAMING_UNCOMPRESSED_RAW_SIZE_BYTES;
    }

    public static final ManagedStreamingQueuingPolicy Default = new ManagedStreamingQueuingPolicy(1);
}
