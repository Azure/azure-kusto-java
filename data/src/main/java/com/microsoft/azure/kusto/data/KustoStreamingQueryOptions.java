// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

/**
 * Options for executing a streaming query via
 * {@link Client#executeQuery(String, String, ClientRequestProperties, KustoStreamingQueryOptions)}.
 * <p>
 * When this parameter is provided, the query is executed using the V2 streaming endpoint and the
 * response is returned as a {@link KustoResponseDataSetV2} that can be iterated row-by-row without
 * loading the entire result set into memory.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Stream with default options (row-by-row)
 * KustoResponseDataSetV2 result = client.executeQuery(db, query, props, KustoStreamingQueryOptions.create());
 *
 * // Stream with a suggested batch size for chunked reading
 * KustoResponseDataSetV2 result = client.executeQuery(db, query, props,
 *     KustoStreamingQueryOptions.withMaxBatchRowCount(500));
 * }</pre>
 *
 * @see Client#executeQuery(String, String, ClientRequestProperties, KustoStreamingQueryOptions)
 * @see KustoResponseDataSetV2
 * @see StreamingDataTable#nextRows(int)
 */
public class KustoStreamingQueryOptions {

    /**
     * The Kusto service enforces a maximum response size of 64 MB. This constant can be used
     * as a reference when choosing batch sizes.
     */
    public static final long MAX_RESPONSE_SIZE_BYTES = 64 * 1024 * 1024;

    private final int maxBatchRowCount;

    private KustoStreamingQueryOptions(int maxBatchRowCount) {
        this.maxBatchRowCount = maxBatchRowCount;
    }

    /**
     * Creates streaming query options with default settings.
     * Rows are consumed one at a time via {@link StreamingDataTable#nextRow()}.
     */
    public static KustoStreamingQueryOptions create() {
        return new KustoStreamingQueryOptions(0);
    }

    /**
     * Creates streaming query options with a suggested batch size for chunked row reading.
     * Use {@link StreamingDataTable#nextRows()} to read rows in batches of this size.
     *
     * @param maxBatchRowCount maximum number of rows per batch (must be positive)
     * @return configured options instance
     * @throws IllegalArgumentException if maxBatchRowCount is not positive
     */
    public static KustoStreamingQueryOptions withMaxBatchRowCount(int maxBatchRowCount) {
        if (maxBatchRowCount <= 0) {
            throw new IllegalArgumentException("maxBatchRowCount must be positive, got " + maxBatchRowCount);
        }
        return new KustoStreamingQueryOptions(maxBatchRowCount);
    }

    /**
     * Returns the maximum number of rows to return per batch, or 0 if no batch size was specified.
     */
    public int getMaxBatchRowCount() {
        return maxBatchRowCount;
    }

    /**
     * Returns whether a batch row count has been configured.
     */
    public boolean hasBatchRowCount() {
        return maxBatchRowCount > 0;
    }
}
