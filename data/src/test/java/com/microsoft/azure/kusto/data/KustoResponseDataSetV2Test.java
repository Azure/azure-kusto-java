// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KustoResponseDataSetV2Test {

    private static InputStream toStream(String json) {
        return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    }

    // -- Minimal valid V2 response with no data tables --

    @Test
    @DisplayName("Parse empty V2 response with no tables")
    void emptyResponse() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertEquals("v2.0", response.getVersion());
            assertFalse(response.isProgressive());
            assertFalse(response.hasNextTable());
            assertTrue(response.isDataSetCompleted());
            assertFalse(response.hasErrors());
            assertFalse(response.isCancelled());
        }
    }

    // -- Single PrimaryResult table with rows --

    @Test
    @DisplayName("Parse single PrimaryResult table with rows")
    void singleTableWithRows() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[{\"ColumnName\":\"Name\",\"ColumnType\":\"string\"},{\"ColumnName\":\"Age\",\"ColumnType\":\"int\"}]," +
                "\"Rows\":[[\"Alice\",30],[\"Bob\",25]]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());

            StreamingDataTable table = response.getTable();
            assertEquals("PrimaryResult", table.getTableName());
            assertEquals(0, table.getTableId());
            assertEquals(WellKnownDataSet.PrimaryResult, table.getTableKind());
            assertEquals(2, table.getColumns().length);
            assertEquals("Name", table.getColumns()[0].getColumnName());
            assertEquals("string", table.getColumns()[0].getColumnType());
            assertEquals("Age", table.getColumns()[1].getColumnName());
            assertEquals("int", table.getColumns()[1].getColumnType());

            // First row
            assertTrue(table.hasNextRow());
            List<Object> row1 = table.nextRow();
            assertEquals("Alice", row1.get(0));
            assertEquals(30, row1.get(1));

            // Second row
            assertTrue(table.hasNextRow());
            List<Object> row2 = table.nextRow();
            assertEquals("Bob", row2.get(0));
            assertEquals(25, row2.get(1));

            // No more rows
            assertFalse(table.hasNextRow());
            assertTrue(table.isExhausted());

            // No more tables
            assertFalse(response.hasNextTable());
            assertFalse(response.hasErrors());
        }
    }

    // -- Multiple tables --

    @Test
    @DisplayName("Parse multi-table response with QueryProperties and PrimaryResult")
    void multiTableResponse() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"QueryProperties\"," +
                "\"TableName\":\"@ExtendedProperties\"," +
                "\"Columns\":[{\"ColumnName\":\"Key\",\"ColumnType\":\"string\"}]," +
                "\"Rows\":[[\"SomeKey\"]]}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":1,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[{\"ColumnName\":\"Value\",\"ColumnType\":\"long\"}]," +
                "\"Rows\":[[42],[100]]}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":2,\"TableKind\":\"QueryCompletionInformation\"," +
                "\"TableName\":\"QueryCompletionInformation\"," +
                "\"Columns\":[{\"ColumnName\":\"Status\",\"ColumnType\":\"string\"}]," +
                "\"Rows\":[[\"OK\"]]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            // First table: QueryProperties
            assertTrue(response.hasNextTable());
            StreamingDataTable t1 = response.getTable();
            assertEquals("@ExtendedProperties", t1.getTableName());
            assertEquals(WellKnownDataSet.QueryProperties, t1.getTableKind());
            assertTrue(t1.hasNextRow());
            assertEquals("SomeKey", t1.nextRow().get(0));
            assertFalse(t1.hasNextRow());

            // Second table: PrimaryResult
            assertTrue(response.hasNextTable());
            StreamingDataTable t2 = response.getTable();
            assertEquals("PrimaryResult", t2.getTableName());
            assertEquals(WellKnownDataSet.PrimaryResult, t2.getTableKind());
            List<Object> r1 = t2.nextRow();
            assertEquals(42, r1.get(0));
            List<Object> r2 = t2.nextRow();
            assertEquals(100, r2.get(0));
            assertFalse(t2.hasNextRow());

            // Third table: QueryCompletionInformation
            assertTrue(response.hasNextTable());
            StreamingDataTable t3 = response.getTable();
            assertEquals(WellKnownDataSet.QueryCompletionInformation, t3.getTableKind());
            assertEquals("OK", t3.nextRow().get(0));
            assertFalse(t3.hasNextRow());

            // Done
            assertFalse(response.hasNextTable());
        }
    }

    // -- Skip rows when advancing tables --

    @Test
    @DisplayName("Skipping to next table auto-skips remaining rows")
    void skipRows() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"QueryProperties\"," +
                "\"TableName\":\"Props\"," +
                "\"Columns\":[{\"ColumnName\":\"K\",\"ColumnType\":\"string\"}]," +
                "\"Rows\":[[\"v1\"],[\"v2\"],[\"v3\"]]}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":1,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"Main\"," +
                "\"Columns\":[{\"ColumnName\":\"X\",\"ColumnType\":\"int\"}]," +
                "\"Rows\":[[1]]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            // First table - read only 1 of 3 rows, then skip to next
            assertTrue(response.hasNextTable());
            StreamingDataTable t1 = response.getTable();
            assertEquals("v1", t1.nextRow().get(0));
            // Don't read remaining rows - hasNextTable should skip them

            // Second table
            assertTrue(response.hasNextTable());
            StreamingDataTable t2 = response.getTable();
            assertEquals(WellKnownDataSet.PrimaryResult, t2.getTableKind());
            assertEquals(1, t2.nextRow().get(0));
            assertFalse(t2.hasNextRow());

            assertFalse(response.hasNextTable());
        }
    }

    // -- Empty rows table --

    @Test
    @DisplayName("Parse table with empty rows")
    void emptyRowsTable() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[{\"ColumnName\":\"X\",\"ColumnType\":\"string\"}]," +
                "\"Rows\":[]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();
            assertFalse(table.hasNextRow());
            assertFalse(response.hasNextTable());
        }
    }

    // -- Data types --

    @Test
    @DisplayName("Parse various data types in rows")
    void variousDataTypes() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[" +
                "{\"ColumnName\":\"str\",\"ColumnType\":\"string\"}," +
                "{\"ColumnName\":\"num_int\",\"ColumnType\":\"int\"}," +
                "{\"ColumnName\":\"num_long\",\"ColumnType\":\"long\"}," +
                "{\"ColumnName\":\"num_real\",\"ColumnType\":\"real\"}," +
                "{\"ColumnName\":\"bool_val\",\"ColumnType\":\"bool\"}," +
                "{\"ColumnName\":\"null_val\",\"ColumnType\":\"string\"}" +
                "]," +
                "\"Rows\":[[\"hello\",42,9999999999,3.14,true,null]]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();
            List<Object> row = table.nextRow();

            assertEquals("hello", row.get(0));
            assertEquals(42, row.get(1));
            assertEquals(9999999999L, row.get(2));
            assertTrue(row.get(3) instanceof BigDecimal);
            assertEquals(new BigDecimal("3.14"), row.get(3));
            assertEquals(true, row.get(4));
            assertNull(row.get(5));
        }
    }

    // -- Error handling: DataSetCompletion with errors --

    @Test
    @DisplayName("Parse DataSetCompletion with errors")
    void dataSetCompletionWithErrors() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[{\"ColumnName\":\"X\",\"ColumnType\":\"string\"}]," +
                "\"Rows\":[]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":true,\"Cancelled\":false," +
                "\"OneApiErrors\":[{\"error\":{\"code\":\"General_BadRequest\"," +
                "\"message\":\"Request is invalid.\",\"@type\":\"Kusto.Data.Exceptions.KustoBadRequestException\"," +
                "\"@message\":\"Request is invalid and cannot be processed\"}}]}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            // Read through the table
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();
            assertFalse(table.hasNextRow());

            // Now DataSetCompletion is processed
            assertFalse(response.hasNextTable());
            assertTrue(response.hasErrors());
            assertFalse(response.isCancelled());
            assertNotNull(response.getOneApiErrors());
            assertFalse(response.getOneApiErrors().isEmpty());
        }
    }

    // -- Cancelled query --

    @Test
    @DisplayName("Parse cancelled query response")
    void cancelledQuery() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":true}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertFalse(response.hasNextTable());
            assertFalse(response.hasErrors());
            assertTrue(response.isCancelled());
        }
    }

    // -- Column with fallback DataType property --

    @Test
    @DisplayName("Parse columns using DataType fallback property name")
    void columnDataTypeFallback() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[{\"ColumnName\":\"Id\",\"DataType\":\"Int32\"}]," +
                "\"Rows\":[[1]]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();
            assertEquals("Int32", table.getColumns()[0].getColumnType());
            assertEquals(1, table.nextRow().get(0));
        }
    }

    // -- Unknown TableKind doesn't crash --

    @Test
    @DisplayName("Unknown TableKind is handled gracefully")
    void unknownTableKind() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"QueryTraceLog\"," +
                "\"TableName\":\"TraceLog\"," +
                "\"Columns\":[{\"ColumnName\":\"Msg\",\"ColumnType\":\"string\"}]," +
                "\"Rows\":[[\"trace message\"]]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();
            // QueryTraceLog is not in WellKnownDataSet enum, so tableKind should be null
            assertNull(table.getTableKind());
            assertEquals("trace message", table.nextRow().get(0));
        }
    }

    // -- getTable() before hasNextTable() throws --

    @Test
    @DisplayName("getTable() before hasNextTable() throws IllegalStateException")
    void getTableBeforeHasNext() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertThrows(IllegalStateException.class, response::getTable);
        }
    }

    // -- nextRow() when exhausted throws --

    @Test
    @DisplayName("nextRow() when exhausted throws IllegalStateException")
    void nextRowWhenExhausted() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[{\"ColumnName\":\"X\",\"ColumnType\":\"string\"}]," +
                "\"Rows\":[]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();
            assertFalse(table.hasNextRow());
            assertThrows(IllegalStateException.class, table::nextRow);
        }
    }

    // -- Realistic response from KustoOperationResultTest --

    @Test
    @DisplayName("Parse realistic V2 response matching existing test data format")
    void realisticResponse() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\",\"IsFragmented\":false,\"ErrorReportingPlacement\":\"EndOfDataSet\"},"
                +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"QueryProperties\",\"TableName\":\"@ExtendedProperties\"," +
                "\"Columns\":[{\"ColumnName\":\"TableId\",\"ColumnType\":\"int\"},{\"ColumnName\":\"Key\",\"ColumnType\":\"string\"},{\"ColumnName\":\"Value\",\"ColumnType\":\"dynamic\"}],"
                +
                "\"Rows\":[[1,\"Visualization\",\"{\\\"Visualization\\\":null}\"]]}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":1,\"TableKind\":\"PrimaryResult\",\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[{\"ColumnName\":\"SourceId\",\"ColumnType\":\"string\"},{\"ColumnName\":\"count_\",\"ColumnType\":\"long\"},{\"ColumnName\":\"EventText\",\"ColumnType\":\"string\"}],"
                +
                "\"Rows\":[]}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":2,\"TableKind\":\"QueryCompletionInformation\",\"TableName\":\"QueryCompletionInformation\"," +
                "\"Columns\":[{\"ColumnName\":\"Timestamp\",\"ColumnType\":\"datetime\"},{\"ColumnName\":\"Level\",\"ColumnType\":\"int\"}]," +
                "\"Rows\":[[\"2024-01-15T19:45:16Z\",4]]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            // Table 0: QueryProperties
            assertTrue(response.hasNextTable());
            StreamingDataTable t0 = response.getTable();
            assertEquals(WellKnownDataSet.QueryProperties, t0.getTableKind());
            List<Object> propRow = t0.nextRow();
            assertEquals(1, propRow.get(0));
            assertEquals("Visualization", propRow.get(1));
            assertFalse(t0.hasNextRow());

            // Table 1: PrimaryResult (empty)
            assertTrue(response.hasNextTable());
            StreamingDataTable t1 = response.getTable();
            assertEquals(WellKnownDataSet.PrimaryResult, t1.getTableKind());
            assertEquals(3, t1.getColumns().length);
            assertFalse(t1.hasNextRow());

            // Table 2: QueryCompletionInformation
            assertTrue(response.hasNextTable());
            StreamingDataTable t2 = response.getTable();
            assertEquals(WellKnownDataSet.QueryCompletionInformation, t2.getTableKind());
            List<Object> infoRow = t2.nextRow();
            assertEquals("2024-01-15T19:45:16Z", infoRow.get(0));
            assertEquals(4, infoRow.get(1));

            // Done
            assertFalse(response.hasNextTable());
            assertFalse(response.hasErrors());
        }
    }

    // -- Multiple rows streamed one at a time --

    @Test
    @DisplayName("Stream many rows one at a time")
    void manyRows() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append("{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"},");
        sb.append("{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\",");
        sb.append("\"TableName\":\"PrimaryResult\",");
        sb.append("\"Columns\":[{\"ColumnName\":\"i\",\"ColumnType\":\"int\"}],");
        sb.append("\"Rows\":[");
        int rowCount = 1000;
        for (int i = 0; i < rowCount; i++) {
            if (i > 0)
                sb.append(",");
            sb.append("[").append(i).append("]");
        }
        sb.append("]},");
        sb.append("{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}");
        sb.append("]");

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(sb.toString()))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();

            List<Integer> collected = new ArrayList<>();
            while (table.hasNextRow()) {
                List<Object> row = table.nextRow();
                collected.add((Integer) row.get(0));
            }

            assertEquals(rowCount, collected.size());
            for (int i = 0; i < rowCount; i++) {
                assertEquals(i, collected.get(i));
            }

            assertFalse(response.hasNextTable());
        }
    }

    // -- Boolean false value --

    @Test
    @DisplayName("Parse boolean false values correctly")
    void booleanFalse() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[{\"ColumnName\":\"flag\",\"ColumnType\":\"bool\"}]," +
                "\"Rows\":[[false],[true]]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();
            assertEquals(false, table.nextRow().get(0));
            assertEquals(true, table.nextRow().get(0));
        }
    }

    // -- Nested JSON values in rows --

    @Test
    @DisplayName("Parse nested JSON objects/arrays in row values")
    void nestedJsonValues() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[{\"ColumnName\":\"dynamic_col\",\"ColumnType\":\"dynamic\"}]," +
                "\"Rows\":[[[1,2,3]],[{\"key\":\"value\"}]]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();

            // Array value
            List<Object> row1 = table.nextRow();
            assertEquals("[1,2,3]", row1.get(0));

            // Object value
            List<Object> row2 = table.nextRow();
            assertEquals("{\"key\":\"value\"}", row2.get(0));
        }
    }

    // -- Invalid response: not a JSON array --

    @Test
    @DisplayName("Invalid response - not a JSON array")
    void invalidResponseNotArray() {
        String json = "{\"error\":\"something\"}";
        assertThrows(IOException.class, () -> new KustoResponseDataSetV2(toStream(json)));
    }

    // -- Invalid response: first frame is not DataSetHeader --

    @Test
    @DisplayName("Invalid response - first frame is not DataSetHeader")
    void invalidFirstFrame() {
        String json = "[{\"FrameType\":\"DataTable\",\"TableId\":0}]";
        assertThrows(IOException.class, () -> new KustoResponseDataSetV2(toStream(json)));
    }

    // -- Column ordinals --

    @Test
    @DisplayName("Column ordinals are correctly assigned")
    void columnOrdinals() throws IOException {
        String json = "[" +
                "{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"}," +
                "{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\"," +
                "\"TableName\":\"PrimaryResult\"," +
                "\"Columns\":[{\"ColumnName\":\"A\",\"ColumnType\":\"string\"},{\"ColumnName\":\"B\",\"ColumnType\":\"int\"},{\"ColumnName\":\"C\",\"ColumnType\":\"bool\"}],"
                +
                "\"Rows\":[]}," +
                "{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}" +
                "]";

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();

            assertEquals(0, table.getColumns()[0].getOrdinal());
            assertEquals(1, table.getColumns()[1].getOrdinal());
            assertEquals(2, table.getColumns()[2].getOrdinal());

            // Column lookup by name
            assertNotNull(table.getColumnsByName().get("A"));
            assertNotNull(table.getColumnsByName().get("B"));
            assertNotNull(table.getColumnsByName().get("C"));
            assertNull(table.getColumnsByName().get("D"));
        }
    }

    // -- Batch reading with nextRows(int) --

    @Test
    @DisplayName("nextRows reads rows in batches")
    void nextRowsBatch() throws IOException {
        String json = buildRowsJson(10);

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();

            // Read in batches of 3
            List<List<Object>> batch1 = table.nextRows(3);
            assertEquals(3, batch1.size());
            assertEquals(0, batch1.get(0).get(0));
            assertEquals(2, batch1.get(2).get(0));

            List<List<Object>> batch2 = table.nextRows(3);
            assertEquals(3, batch2.size());
            assertEquals(3, batch2.get(0).get(0));

            List<List<Object>> batch3 = table.nextRows(3);
            assertEquals(3, batch3.size());

            // Last batch has only 1 row
            List<List<Object>> batch4 = table.nextRows(3);
            assertEquals(1, batch4.size());
            assertEquals(9, batch4.get(0).get(0));

            // No more rows
            List<List<Object>> batch5 = table.nextRows(3);
            assertTrue(batch5.isEmpty());
        }
    }

    @Test
    @DisplayName("nextRows() uses default batch size from options")
    void nextRowsDefaultBatch() throws IOException {
        String json = buildRowsJson(7);

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            response.setDefaultBatchRowCount(4);
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();

            List<List<Object>> batch1 = table.nextRows();
            assertEquals(4, batch1.size());

            List<List<Object>> batch2 = table.nextRows();
            assertEquals(3, batch2.size());

            List<List<Object>> batch3 = table.nextRows();
            assertTrue(batch3.isEmpty());
        }
    }

    @Test
    @DisplayName("nextRows() without batch size reads all remaining rows")
    void nextRowsNoBatchReadsAll() throws IOException {
        String json = buildRowsJson(5);

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();

            // No batch size set — reads all
            List<List<Object>> all = table.nextRows();
            assertEquals(5, all.size());
        }
    }

    @Test
    @DisplayName("nextRows with invalid batchSize throws")
    void nextRowsInvalidBatchSize() throws IOException {
        String json = buildRowsJson(1);

        try (KustoResponseDataSetV2 response = new KustoResponseDataSetV2(toStream(json))) {
            assertTrue(response.hasNextTable());
            StreamingDataTable table = response.getTable();
            assertThrows(IllegalArgumentException.class, () -> table.nextRows(0));
            assertThrows(IllegalArgumentException.class, () -> table.nextRows(-1));
        }
    }

    // -- KustoStreamingQueryOptions --

    @Test
    @DisplayName("KustoStreamingQueryOptions factory methods")
    void streamingQueryOptions() {
        KustoStreamingQueryOptions defaults = KustoStreamingQueryOptions.create();
        assertFalse(defaults.hasBatchRowCount());
        assertEquals(0, defaults.getMaxBatchRowCount());

        KustoStreamingQueryOptions withBatch = KustoStreamingQueryOptions.withMaxBatchRowCount(500);
        assertTrue(withBatch.hasBatchRowCount());
        assertEquals(500, withBatch.getMaxBatchRowCount());

        assertThrows(IllegalArgumentException.class, () -> KustoStreamingQueryOptions.withMaxBatchRowCount(0));
        assertThrows(IllegalArgumentException.class, () -> KustoStreamingQueryOptions.withMaxBatchRowCount(-1));
    }

    // -- Helper to build a simple N-row response --

    private static String buildRowsJson(int rowCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append("{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":false,\"Version\":\"v2.0\"},");
        sb.append("{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"PrimaryResult\",");
        sb.append("\"TableName\":\"PrimaryResult\",");
        sb.append("\"Columns\":[{\"ColumnName\":\"i\",\"ColumnType\":\"int\"}],");
        sb.append("\"Rows\":[");
        for (int i = 0; i < rowCount; i++) {
            if (i > 0)
                sb.append(",");
            sb.append("[").append(i).append("]");
        }
        sb.append("]},");
        sb.append("{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}");
        sb.append("]");
        return sb.toString();
    }
}
