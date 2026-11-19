// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Represents a single table in a V2 streaming query response, providing row-by-row iteration
 * without buffering all rows in memory.
 * <p>
 * Instances are created by {@link KustoResponseDataSetV2} and must be consumed (or closed) before
 * advancing to the next table. Rows are read directly from the underlying {@link JsonParser}.
 * <p>
 * Example usage:
 * <pre>{@code
 * try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(inputStream)) {
 *     while (response.hasNextTable()) {
 *         StreamingDataTable table = response.getTable();
 *         KustoResultColumn[] columns = table.getColumns();
 *         while (table.hasNextRow()) {
 *             List<Object> row = table.nextRow();
 *             // process row
 *         }
 *     }
 * }
 * }</pre>
 *
 * @see KustoResponseDataSetV2
 */
public class StreamingDataTable {

    private static final String EXCEPTIONS_PROPERTY_NAME = "Exceptions";

    private final JsonParser parser;
    private final String tableName;
    private final int tableId;
    private final WellKnownDataSet tableKind;
    private final KustoResultColumn[] columns;
    private final Map<String, KustoResultColumn> columnsByName;

    private boolean rowsStarted;
    private boolean exhausted;
    private boolean nextRowPeeked;
    private int batchRowCount;

    StreamingDataTable(JsonParser parser, String tableName, int tableId, WellKnownDataSet tableKind,
            KustoResultColumn[] columns) {
        this.parser = parser;
        this.tableName = tableName;
        this.tableId = tableId;
        this.tableKind = tableKind;
        this.columns = columns;
        this.columnsByName = new HashMap<>();
        for (KustoResultColumn col : columns) {
            columnsByName.put(col.getColumnName(), col);
        }
        this.rowsStarted = false;
        this.exhausted = false;
        this.nextRowPeeked = false;
    }

    /**
     * Returns the table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the table's numeric identifier.
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * Returns the table kind (e.g., PrimaryResult, QueryCompletionInformation).
     */
    public WellKnownDataSet getTableKind() {
        return tableKind;
    }

    /**
     * Returns the column definitions for this table.
     */
    public KustoResultColumn[] getColumns() {
        return columns;
    }

    /**
     * Returns the column map by name.
     */
    public Map<String, KustoResultColumn> getColumnsByName() {
        return columnsByName;
    }

    /**
     * Returns true if there is another row to read from this table.
     *
     * @throws IOException if an I/O error occurs reading the stream
     */
    public boolean hasNextRow() throws IOException {
        if (exhausted) {
            return false;
        }

        if (!rowsStarted) {
            // Parser is positioned at the START_ARRAY of the Rows array
            JsonToken token = parser.currentToken();
            if (token != JsonToken.START_ARRAY) {
                throw new IOException("Expected START_ARRAY for Rows, got " + token);
            }
            rowsStarted = true;
        }

        if (!nextRowPeeked) {
            JsonToken token = parser.nextToken();
            if (token == JsonToken.END_ARRAY) {
                exhausted = true;
                return false;
            }
            // token should be START_ARRAY (row) or START_OBJECT (error row)
            nextRowPeeked = true;
        }

        return true;
    }

    /**
     * Reads and returns the next row as a list of values.
     * <p>
     * The values in the list are typed according to their JSON representation:
     * strings as {@link String}, booleans as {@link Boolean}, integers as {@link Integer} or
     * {@link Long}, decimals as {@link java.math.BigDecimal}, and nulls as {@code null}.
     *
     * @return the next row
     * @throws IOException              if an I/O error occurs
     * @throws IllegalStateException    if no more rows are available
     */
    public List<Object> nextRow() throws IOException {
        if (!nextRowPeeked && !hasNextRow()) {
            throw new IllegalStateException("No more rows available");
        }
        nextRowPeeked = false;

        JsonToken currentToken = parser.currentToken();

        // Error rows are JSON objects with "Exceptions" or "OneApiErrors"
        if (currentToken == JsonToken.START_OBJECT) {
            handleErrorRow();
        }

        // Normal rows are JSON arrays
        if (currentToken != JsonToken.START_ARRAY) {
            throw new IOException("Expected START_ARRAY for row, got " + currentToken);
        }

        List<Object> row = new ArrayList<>(columns.length);
        JsonToken token;
        while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
            row.add(readValue(token));
        }
        return row;
    }

    private Object readValue(JsonToken token) throws IOException {
        if (token == null) {
            throw new IOException("Unexpected end of stream while reading row value");
        }
        switch (token) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return parser.getText();
            case VALUE_TRUE:
                return Boolean.TRUE;
            case VALUE_FALSE:
                return Boolean.FALSE;
            case VALUE_NUMBER_INT:
                // Match existing KustoResultSetTable behavior: int if fits, else long
                long longVal = parser.getLongValue();
                if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
                    return (int) longVal;
                }
                return longVal;
            case VALUE_NUMBER_FLOAT:
                // Match existing behavior: use BigDecimal for precision
                return parser.getDecimalValue();
            case START_ARRAY:
            case START_OBJECT:
                // Complex nested values - read as tree and return as string representation
                return parser.readValueAsTree().toString();
            default:
                throw new IOException("Unexpected token in row: " + token);
        }
    }

    private void handleErrorRow() throws IOException {
        // Read the error object as a tree to extract the error details
        com.fasterxml.jackson.databind.JsonNode errorNode = parser.readValueAsTree();
        if (errorNode.has(EXCEPTIONS_PROPERTY_NAME)) {
            com.fasterxml.jackson.databind.node.ArrayNode exceptions = (com.fasterxml.jackson.databind.node.ArrayNode) errorNode.get(EXCEPTIONS_PROPERTY_NAME);
            throw new IOException("Query error in row: " + exceptions.toString());
        }
        if (errorNode.has(KustoOperationResult.ONE_API_ERRORS_PROPERTY_NAME)) {
            com.fasterxml.jackson.databind.node.ArrayNode errors = (com.fasterxml.jackson.databind.node.ArrayNode) errorNode
                    .get(KustoOperationResult.ONE_API_ERRORS_PROPERTY_NAME);
            throw new IOException("OneAPI error in row: " + errors.toString());
        }
        throw new IOException("Unexpected object in Rows array: " + errorNode.toString());
    }

    /**
     * Reads up to {@code batchSize} rows and returns them as a list.
     * Returns fewer rows if the table is exhausted before the batch is full.
     * Returns an empty list if no more rows are available.
     * <p>
     * This is a convenience method for chunked processing of large result sets.
     *
     * @param batchSize maximum number of rows to read (must be positive)
     * @return a list of rows, each row being a list of column values
     * @throws IOException              if an I/O error occurs
     * @throws IllegalArgumentException if batchSize is not positive
     */
    public List<List<Object>> nextRows(int batchSize) throws IOException {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive, got " + batchSize);
        }
        List<List<Object>> batch = new ArrayList<>(Math.min(batchSize, 1024));
        while (batch.size() < batchSize && hasNextRow()) {
            batch.add(nextRow());
        }
        return batch;
    }

    /**
     * Reads up to {@code options.getMaxBatchRowCount()} rows and returns them as a list.
     * If the options do not specify a batch size, reads all remaining rows.
     * <p>
     * This is a convenience method for use with
     * {@link Client#executeQuery(String, String, ClientRequestProperties, KustoStreamingQueryOptions)}.
     *
     * @return a list of rows
     * @throws IOException if an I/O error occurs
     * @see KustoStreamingQueryOptions#withMaxBatchRowCount(int)
     */
    public List<List<Object>> nextRows() throws IOException {
        if (batchRowCount > 0) {
            return nextRows(batchRowCount);
        }
        // No batch size configured — read all remaining rows
        List<List<Object>> all = new ArrayList<>();
        while (hasNextRow()) {
            all.add(nextRow());
        }
        return all;
    }

    /**
     * Returns whether all rows have been consumed.
     */
    public boolean isExhausted() {
        return exhausted;
    }

    /**
     * Sets the default batch row count used by {@link #nextRows()}.
     * This is typically set by {@link KustoResponseDataSetV2} from the query options.
     *
     * @param batchRowCount the max rows per batch
     */
    void setBatchRowCount(int batchRowCount) {
        this.batchRowCount = batchRowCount;
    }

    /**
     * Skips all remaining rows in this table. Call this if you want to advance to the next table
     * without reading all rows.
     *
     * @throws IOException if an I/O error occurs
     */
    public void skipRemainingRows() throws IOException {
        while (hasNextRow()) {
            skipCurrentRow();
        }
    }

    private void skipCurrentRow() throws IOException {
        nextRowPeeked = false;
        JsonToken token = parser.currentToken();
        if (token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT) {
            parser.skipChildren();
        }
    }
}
