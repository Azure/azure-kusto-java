// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;

/**
 * Streaming parser for Kusto V2 query responses. Reads the response from an {@link InputStream}
 * incrementally using Jackson's streaming API, yielding rows one at a time without buffering
 * the entire response in memory.
 * <p>
 * This class is designed to be used with {@link StreamingClient#executeStreamingQuery} which
 * returns the raw V2 response stream. The V2 response format is a JSON array of frames:
 * {@code [DataSetHeader, DataTable*, DataSetCompletion]}.
 * <p>
 * Example usage:
 * <pre>{@code
 * InputStream stream = client.executeStreamingQuery(database, query, properties);
 * try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(stream)) {
 *     while (response.hasNextTable()) {
 *         StreamingDataTable table = response.getTable();
 *         System.out.println("Table: " + table.getTableName() + " Kind: " + table.getTableKind());
 *         KustoResultColumn[] columns = table.getColumns();
 *         while (table.hasNextRow()) {
 *             List<Object> row = table.nextRow();
 *             // process each row without loading all into memory
 *         }
 *     }
 *     if (response.hasErrors()) {
 *         // handle errors
 *     }
 * }
 * }</pre>
 *
 * @see StreamingClient#executeStreamingQuery(String, String, ClientRequestProperties)
 * @see StreamingDataTable
 * @see <a href="https://learn.microsoft.com/en-us/kusto/api/rest/response-v2">V2 Response Format</a>
 */
public class KustoResponseDataSetV2 implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KustoResponseDataSetV2.class);

    private static final String FRAME_TYPE_PROPERTY = "FrameType";
    private static final String VERSION_PROPERTY = "Version";
    private static final String IS_PROGRESSIVE_PROPERTY = "IsProgressive";
    private static final String TABLE_ID_PROPERTY = "TableId";
    private static final String TABLE_KIND_PROPERTY = "TableKind";
    private static final String TABLE_NAME_PROPERTY = "TableName";
    private static final String COLUMNS_PROPERTY = "Columns";
    private static final String COLUMN_NAME_PROPERTY = "ColumnName";
    private static final String COLUMN_TYPE_PROPERTY = "ColumnType";
    private static final String COLUMN_TYPE_ALT_PROPERTY = "DataType";
    private static final String ROWS_PROPERTY = "Rows";
    private static final String HAS_ERRORS_PROPERTY = "HasErrors";
    private static final String CANCELLED_PROPERTY = "Cancelled";
    private static final String ONE_API_ERRORS_PROPERTY = "OneApiErrors";

    private final JsonParser parser;
    private final InputStream inputStream;
    private final ObjectMapper mapper;

    private String version;
    private boolean isProgressive;

    // DataSetCompletion state
    private boolean dataSetCompleted;
    private boolean hasErrors;
    private boolean cancelled;
    private List<RuntimeException> oneApiErrors;

    // Current streaming table
    private StreamingDataTable currentTable;
    private int defaultBatchRowCount;

    /**
     * Creates a new streaming V2 response parser.
     * <p>
     * The constructor reads and validates the DataSetHeader frame. The stream must be positioned
     * at the beginning of the V2 JSON response.
     *
     * @param inputStream the V2 response stream (e.g., from executeStreamingQuery)
     * @throws IOException if the stream cannot be read or the DataSetHeader is invalid
     */
    public KustoResponseDataSetV2(InputStream inputStream) throws IOException {
        this.inputStream = inputStream;
        this.mapper = Utils.getObjectMapper();
        JsonFactory factory = mapper.getFactory();
        this.parser = factory.createParser(inputStream);

        // Read opening array bracket
        JsonToken startToken = parser.nextToken();
        if (startToken != JsonToken.START_ARRAY) {
            throw new IOException("Expected JSON array at start of V2 response, got " + startToken);
        }

        // Read and process DataSetHeader
        readDataSetHeader();
    }

    private void readDataSetHeader() throws IOException {
        JsonToken token = parser.nextToken();
        if (token != JsonToken.START_OBJECT) {
            throw new IOException("Expected DataSetHeader object, got " + token);
        }

        String frameType = null;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken(); // move to value
            switch (fieldName) {
                case FRAME_TYPE_PROPERTY:
                    frameType = parser.getText();
                    break;
                case VERSION_PROPERTY:
                    version = parser.getText();
                    break;
                case IS_PROGRESSIVE_PROPERTY:
                    isProgressive = parser.getBooleanValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        if (!FrameType.DataSetHeader.name().equals(frameType)) {
            throw new IOException("First frame must be DataSetHeader, got " + frameType);
        }
    }

    /**
     * Returns the protocol version from the DataSetHeader (e.g., "v2.0").
     */
    public String getVersion() {
        return version;
    }

    /**
     * Returns whether the response uses progressive mode.
     */
    public boolean isProgressive() {
        return isProgressive;
    }

    /**
     * Advances to the next DataTable in the response. Non-table frames are skipped.
     * <p>
     * If the current table has unread rows, they are skipped automatically.
     * When the DataSetCompletion frame is reached (or the stream ends), returns false.
     *
     * @return true if a table is available via {@link #getTable()}, false if no more tables
     * @throws IOException if an I/O error occurs reading the stream
     */
    public boolean hasNextTable() throws IOException {
        // Finish consuming current table if needed
        finishCurrentTable();

        while (true) {
            JsonToken token = parser.nextToken();
            if (token == null || token == JsonToken.END_ARRAY) {
                return false;
            }

            if (token != JsonToken.START_OBJECT) {
                throw new IOException("Expected START_OBJECT for frame, got " + token);
            }

            // Read frame fields to determine type
            FrameFields fields = readFrameFields();

            if (FrameType.DataTable.name().equals(fields.frameType)) {
                currentTable = createStreamingDataTable(fields);
                return true;
            } else if (FrameType.DataSetCompletion.name().equals(fields.frameType)) {
                processDataSetCompletion(fields);
                return false;
            }
            // Skip other frame types (TableHeader, TableFragment, etc.)
            // Their remaining content was already consumed by readFrameFields
        }
    }

    /**
     * Returns the current streaming table. Must be called after {@link #hasNextTable()} returns true.
     *
     * @return the current streaming table
     * @throws IllegalStateException if no table is available
     */
    public StreamingDataTable getTable() {
        if (currentTable == null) {
            throw new IllegalStateException("No table available. Call hasNextTable() first.");
        }
        return currentTable;
    }

    /**
     * Returns whether the DataSetCompletion frame indicated errors.
     * Only valid after iteration is complete (i.e., {@link #hasNextTable()} returned false).
     */
    public boolean hasErrors() {
        return hasErrors;
    }

    /**
     * Returns whether the query was cancelled.
     * Only valid after iteration is complete.
     */
    public boolean isCancelled() {
        return cancelled;
    }

    /**
     * Returns the OneApiErrors from the DataSetCompletion frame, if any.
     * Only valid after iteration is complete and {@link #hasErrors()} is true.
     */
    public List<RuntimeException> getOneApiErrors() {
        return oneApiErrors;
    }

    /**
     * Returns whether the DataSetCompletion frame has been read.
     */
    public boolean isDataSetCompleted() {
        return dataSetCompleted;
    }

    @Override
    public void close() throws IOException {
        parser.close();
        inputStream.close();
    }

    /**
     * Reads all fields of the current frame object. For DataTable frames, stops after positioning
     * the parser at the start of the Rows array (so rows can be streamed). For all other frames,
     * reads the entire object.
     */
    private FrameFields readFrameFields() throws IOException {
        FrameFields fields = new FrameFields();

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken(); // move to value

            switch (fieldName) {
                case FRAME_TYPE_PROPERTY:
                    fields.frameType = parser.getText();
                    break;
                case TABLE_ID_PROPERTY:
                    fields.tableId = parser.getIntValue();
                    break;
                case TABLE_KIND_PROPERTY:
                    fields.tableKind = parser.getText();
                    break;
                case TABLE_NAME_PROPERTY:
                    fields.tableName = parser.getText();
                    break;
                case COLUMNS_PROPERTY:
                    fields.columns = readColumns();
                    break;
                case ROWS_PROPERTY:
                    if (FrameType.DataTable.name().equals(fields.frameType)) {
                        // Don't consume the Rows array - leave parser positioned at START_ARRAY
                        // so StreamingDataTable can iterate rows on demand
                        fields.hasStreamingRows = true;
                        return fields;
                    }
                    // For non-DataTable frames, skip the rows
                    parser.skipChildren();
                    break;
                case HAS_ERRORS_PROPERTY:
                    fields.hasErrors = parser.getBooleanValue();
                    break;
                case CANCELLED_PROPERTY:
                    fields.cancelled = parser.getBooleanValue();
                    break;
                case ONE_API_ERRORS_PROPERTY:
                    fields.oneApiErrors = (ArrayNode) mapper.readTree(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return fields;
    }

    private KustoResultColumn[] readColumns() throws IOException {
        if (parser.currentToken() != JsonToken.START_ARRAY) {
            throw new IOException("Expected START_ARRAY for Columns, got " + parser.currentToken());
        }

        List<KustoResultColumn> columnList = new ArrayList<>();
        int ordinal = 0;

        while (parser.nextToken() != JsonToken.END_ARRAY) {
            // Each column is an object: {"ColumnName": "...", "ColumnType": "..."}
            String colName = null;
            String colType = null;

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.currentName();
                parser.nextToken();
                switch (field) {
                    case COLUMN_NAME_PROPERTY:
                        colName = parser.getText();
                        break;
                    case COLUMN_TYPE_PROPERTY:
                        colType = parser.getText();
                        break;
                    case COLUMN_TYPE_ALT_PROPERTY:
                        if (colType == null || colType.isEmpty()) {
                            colType = parser.getText();
                        }
                        break;
                    default:
                        parser.skipChildren();
                        break;
                }
            }

            if (colName == null) {
                throw new IOException("Column definition missing ColumnName");
            }
            columnList.add(new KustoResultColumn(colName, colType != null ? colType : "", ordinal));
            ordinal++;
        }

        return columnList.toArray(new KustoResultColumn[0]);
    }

    private StreamingDataTable createStreamingDataTable(FrameFields fields) {
        WellKnownDataSet kind = null;
        if (fields.tableKind != null && !fields.tableKind.isEmpty()) {
            try {
                kind = WellKnownDataSet.valueOf(fields.tableKind);
            } catch (IllegalArgumentException e) {
                log.debug("Unknown table kind: {}", fields.tableKind);
            }
        }

        StreamingDataTable table = new StreamingDataTable(
                parser,
                fields.tableName,
                fields.tableId,
                kind,
                fields.columns != null ? fields.columns : new KustoResultColumn[0]);
        if (defaultBatchRowCount > 0) {
            table.setBatchRowCount(defaultBatchRowCount);
        }
        return table;
    }

    /**
     * Sets the default batch row count for all tables returned by this data set.
     * Typically called from the streaming query execution path based on
     * {@link KustoStreamingQueryOptions#getMaxBatchRowCount()}.
     *
     * @param batchRowCount the default max rows per batch for {@link StreamingDataTable#nextRows()}
     */
    void setDefaultBatchRowCount(int batchRowCount) {
        this.defaultBatchRowCount = batchRowCount;
    }

    private void processDataSetCompletion(FrameFields fields) {
        dataSetCompleted = true;
        hasErrors = fields.hasErrors;
        cancelled = fields.cancelled;

        if (hasErrors && fields.oneApiErrors != null) {
            KustoServiceQueryError error = KustoServiceQueryError.fromOneApiErrorArray(fields.oneApiErrors, true);
            oneApiErrors = error.getExceptions();
        }
    }

    private void finishCurrentTable() throws IOException {
        if (currentTable != null && !currentTable.isExhausted()) {
            currentTable.skipRemainingRows();
        }
        if (currentTable != null && currentTable.isExhausted()) {
            // After Rows END_ARRAY, skip remaining fields of the DataTable frame object
            skipToEndOfObject();
        }
        currentTable = null;
    }

    /**
     * Skips tokens until the current object's END_OBJECT is reached.
     * This handles the case where a DataTable frame has fields after the Rows array.
     */
    private void skipToEndOfObject() throws IOException {
        int depth = 1;
        JsonToken token;
        while ((token = parser.nextToken()) != null) {
            if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                depth++;
            } else if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
                depth--;
                if (depth == 0) {
                    return;
                }
            }
        }
    }

    /**
     * Intermediate holder for frame fields read during streaming parse.
     */
    private static class FrameFields {
        String frameType;
        int tableId;
        String tableKind;
        String tableName;
        KustoResultColumn[] columns;
        boolean hasStreamingRows;
        boolean hasErrors;
        boolean cancelled;
        ArrayNode oneApiErrors;
    }
}
