package com.microsoft.azure.kusto.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.kusto.data.exceptions.JsonPropertyMissingException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.TimeZone;
import java.util.UUID;

import static com.microsoft.azure.kusto.data.KustoResultSetTable.*;
import static org.junit.jupiter.api.Assertions.*;

public class ResultSetTest {
    private static final String STR_VAL = "str";
    private static final Instant NOW_VAL = Instant.now();
    private static final Duration DURATION_VAL = Duration.ofHours(2).plusSeconds(1);
    private static final BigDecimal DECIMAL_VAL = BigDecimal.valueOf(1, 1);
    private static final UUID UUID_VAL = UUID.randomUUID();
    private static final int INT_VAL = 1;
    private static final long LONG_VAL = 100000000000L;
    private static final double DOUBLE_VAL = 1.1d;
    private static final String JSON_VAL = "{\"JsonField1\":\"JsonValue1\",\"JsonField2\":\"JsonValue2\",\"Rows\":[[true,\"str\"]]}";
    private static final short SHORT_VAL = 10;
    private static final float FLOAT_VAL = 15.0f;
    private static final BigDecimal BIGDECIMAL_VAL = new BigDecimal("10.0003214134245341414141314134134101");
    private static final String DURATION_AS_KUSTO_STRING_VAL = LocalTime.MIDNIGHT.plus(DURATION_VAL).toString();
    private static final byte BYTE_VAL = STR_VAL.getBytes(StandardCharsets.UTF_8)[0];

    private static KustoResultSetTable kustoResultSetTableEmpty;
    private static KustoResultSetTable kustoResultSetTableWithValues;

    @BeforeAll
    public static void setup() throws JsonProcessingException, JsonPropertyMissingException, KustoServiceQueryError {
        JsonNode nullObj = null;
        ObjectMapper objectMapper = Utils.getObjectMapper();

        String columns = "[ " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"a\", \"" + COLUMN_TYPE_PROPERTY_NAME + "\": \"bool\" }, " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"b\", \"" + COLUMN_TYPE_SECOND_PROPERTY_NAME + "\": \"string\" }, " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"c\", \"" + COLUMN_TYPE_PROPERTY_NAME + "\": \"datetime\" }, " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"d\", \"" + COLUMN_TYPE_PROPERTY_NAME + "\": \"decimal\" }, " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"e\", \"" + COLUMN_TYPE_PROPERTY_NAME + "\": \"dynamic\" }, " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"f\", \"" + COLUMN_TYPE_PROPERTY_NAME + "\": \"guid\" }, " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"g\", \"" + COLUMN_TYPE_PROPERTY_NAME + "\": \"int\" }, " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"h\", \"" + COLUMN_TYPE_PROPERTY_NAME + "\": \"long\" }, " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"i\", \"" + COLUMN_TYPE_PROPERTY_NAME + "\": \"real\" }, " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"j\", \"" + COLUMN_TYPE_PROPERTY_NAME + "\": \"timespan\" }, " +
                "{ \"" + COLUMN_NAME_PROPERTY_NAME + "\": \"k\", \"" + COLUMN_TYPE_PROPERTY_NAME + "\": \"short\" } " +
                "]";

        ArrayNode rowEmpty = objectMapper.createArrayNode();
        rowEmpty.add(nullObj);
        rowEmpty.add(nullObj);
        rowEmpty.add(nullObj);
        rowEmpty.add(nullObj);
        rowEmpty.add(nullObj);
        rowEmpty.add(nullObj);
        rowEmpty.add(nullObj);
        rowEmpty.add(nullObj);
        rowEmpty.add(nullObj);
        rowEmpty.add(nullObj);
        rowEmpty.add(nullObj);
        kustoResultSetTableEmpty = new KustoResultSetTable(objectMapper.readTree("{\"" + TABLE_NAME_PROPERTY_NAME + "\":\"TableEmpty\"," +
                "\"" + COLUMNS_PROPERTY_NAME + "\":" + columns + ",\"" + ROWS_PROPERTY_NAME + "\":" +
                objectMapper.createArrayNode().add(rowEmpty) + "}"));
        kustoResultSetTableEmpty.first();

        ArrayNode rowWithValues = objectMapper.createArrayNode();
        rowWithValues.add(true);
        rowWithValues.add(STR_VAL);
        rowWithValues.add(String.valueOf(NOW_VAL));
        rowWithValues.add(DECIMAL_VAL);
        rowWithValues.add(objectMapper.readTree(JSON_VAL));
        rowWithValues.add(String.valueOf(UUID_VAL));
        rowWithValues.add(INT_VAL);
        rowWithValues.add(LONG_VAL);
        rowWithValues.add(BIGDECIMAL_VAL);
        rowWithValues.add(DURATION_AS_KUSTO_STRING_VAL);
        rowWithValues.add(SHORT_VAL);
        rowWithValues.add(BYTE_VAL);
        rowWithValues.add(FLOAT_VAL);
        rowWithValues.add(DOUBLE_VAL);
        kustoResultSetTableWithValues = new KustoResultSetTable(objectMapper.readTree("{\"" + TABLE_ID_PROPERTY_NAME + "\":\"TableWithValues\"," +
                "\"" + COLUMNS_PROPERTY_NAME + "\":" + columns + ",\"" + ROWS_PROPERTY_NAME + "\":" +
                objectMapper.createArrayNode().add(rowWithValues) + "}"));
        kustoResultSetTableWithValues.first();
    }

    @Test
    public void testKustoResultSetBoolean_WhenEmpty_ReturnsNullIfObjectOrThrows() {
        assertNull(kustoResultSetTableEmpty.getBooleanObject(0));
        assertNull(kustoResultSetTableEmpty.getBooleanObject("a"));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getBoolean(0));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getBoolean("a"));
    }

    @Test
    public void testKustoResultSetBoolean_WhenHasValue_ReturnsValue() {
        assertTrue(kustoResultSetTableWithValues.getBooleanObject(0));
        assertTrue(kustoResultSetTableWithValues.getBooleanObject("a"));
        assertTrue(kustoResultSetTableWithValues.getBoolean(0));
        assertTrue(kustoResultSetTableWithValues.getBoolean("a"));
    }

    @Test
    public void testKustoResultSetString_WhenEmpty_ReturnsNull() {
        assertNull(kustoResultSetTableEmpty.getString(1));
        assertNull(kustoResultSetTableEmpty.getString("b"));

        assertNull(kustoResultSetTableEmpty.getBytes(1));
        assertNull(kustoResultSetTableEmpty.getBytes("b"));
    }

    @Test
    public void testKustoResultSetString_WhenHasValue_ReturnsValue() {
        assertEquals(STR_VAL, kustoResultSetTableWithValues.getString(1));
        assertEquals(STR_VAL, kustoResultSetTableWithValues.getString("b"));

        assertArrayEquals(STR_VAL.getBytes(StandardCharsets.UTF_8), kustoResultSetTableWithValues.getBytes(1));
        assertArrayEquals(STR_VAL.getBytes(StandardCharsets.UTF_8), kustoResultSetTableWithValues.getBytes("b"));
    }

    @Test
    public void testKustoResultSetDatetime_WhenEmpty_ReturnsNull() throws SQLException {
        assertNull(kustoResultSetTableEmpty.getTimestamp(2));
        assertNull(kustoResultSetTableEmpty.getTimestamp("c"));

        assertNull(kustoResultSetTableEmpty.getKustoDateTime(2));
        assertNull(kustoResultSetTableEmpty.getKustoDateTime("c"));
    }

    @Test
    public void testKustoResultSetDatetime_WhenHasValue_ReturnsValue() throws SQLException {
        assertEquals(Timestamp.valueOf(NOW_VAL.atZone(ZoneId.of("UTC")).toLocalDateTime()), kustoResultSetTableWithValues.getTimestamp(2));
        assertEquals(Timestamp.valueOf(NOW_VAL.atZone(ZoneId.of("UTC")).toLocalDateTime()), kustoResultSetTableWithValues.getTimestamp("c"));

        assertEquals(LocalDateTime.ofInstant(NOW_VAL, ZoneOffset.UTC), kustoResultSetTableWithValues.getKustoDateTime(2));
        assertEquals(LocalDateTime.ofInstant(NOW_VAL, ZoneOffset.UTC), kustoResultSetTableWithValues.getKustoDateTime("c"));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        assertEquals(sdf.format(new Date(NOW_VAL.getEpochSecond() * 1000)), kustoResultSetTableWithValues.getDate(2).toString());
        assertEquals(sdf.format(new Date(NOW_VAL.getEpochSecond() * 1000)), kustoResultSetTableWithValues.getDate("c").toString());
    }

    @Test
    public void testKustoResultSetDecimal_WhenEmpty_ReturnsNullIfObjectOrThrows() {
        assertNull(kustoResultSetTableEmpty.getBigDecimal(3));
        assertNull(kustoResultSetTableEmpty.getBigDecimal("d"));

        assertNull(kustoResultSetTableEmpty.getFloatObject(3));
        assertNull(kustoResultSetTableEmpty.getFloatObject("d"));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getFloat(3));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getFloat("d"));

        assertNull(kustoResultSetTableEmpty.getDoubleObject(3));
        assertNull(kustoResultSetTableEmpty.getDoubleObject("d"));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getDouble(3));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getDouble("d"));
    }

    @Test
    public void testKustoResultSetDecimal_WhenHasValue_ReturnsValue() {
        assertEquals(DECIMAL_VAL, kustoResultSetTableWithValues.getBigDecimal(3));
        assertEquals(DECIMAL_VAL, kustoResultSetTableWithValues.getBigDecimal("d"));

        assertEquals(DECIMAL_VAL.floatValue(), kustoResultSetTableWithValues.getFloatObject(3));
        assertEquals(DECIMAL_VAL.floatValue(), kustoResultSetTableWithValues.getFloatObject("d"));
        assertEquals(DECIMAL_VAL.floatValue(), kustoResultSetTableWithValues.getFloat(3));
        assertEquals(DECIMAL_VAL.floatValue(), kustoResultSetTableWithValues.getFloat("d"));

        assertEquals(DECIMAL_VAL.doubleValue(), kustoResultSetTableWithValues.getDouble(3));
        assertEquals(DECIMAL_VAL.doubleValue(), kustoResultSetTableWithValues.getDouble("d"));
        assertEquals(DECIMAL_VAL.doubleValue(), kustoResultSetTableWithValues.getDoubleObject(3));
        assertEquals(DECIMAL_VAL.doubleValue(), kustoResultSetTableWithValues.getDoubleObject("d"));
    }

    @Test
    public void testKustoResultSetDynamic_WhenEmpty_ReturnsNull() {
        assertNull(kustoResultSetTableEmpty.getJSONObject(4));
        assertNull(kustoResultSetTableEmpty.getJSONObject("e"));
    }

    @Test
    public void testKustoResultSetDynamic_WhenHasValue_ReturnsValue() throws JsonProcessingException {
        ObjectMapper objectMapper = Utils.getObjectMapper();
        assertEquals(objectMapper.readTree(JSON_VAL), kustoResultSetTableWithValues.getJSONObject(4));
        assertEquals(objectMapper.readTree(JSON_VAL), kustoResultSetTableWithValues.getJSONObject("e"));
    }

    @Test
    public void testKustoResultSetGuid_WhenEmpty_ReturnsNull() {
        assertNull(kustoResultSetTableEmpty.getUUID(5));
        assertNull(kustoResultSetTableEmpty.getUUID("f"));
    }

    @Test
    public void testKustoResultSetGuid_WhenHasValue_ReturnsValue() {
        assertEquals(UUID_VAL, kustoResultSetTableWithValues.getUUID(5));
        assertEquals(UUID_VAL, kustoResultSetTableWithValues.getUUID("f"));
    }

    @Test
    public void testKustoResultSetInteger_WhenEmpty_ReturnsNullIfObjectOrThrows() {
        assertNull(kustoResultSetTableEmpty.getIntegerObject(6));
        assertNull(kustoResultSetTableEmpty.getIntegerObject("g"));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getInt(6));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getInt("g"));

        assertNull(kustoResultSetTableEmpty.getLongObject(6));
        assertNull(kustoResultSetTableEmpty.getLongObject("g"));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getLong(6));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getLong("g"));

        assertNull(kustoResultSetTableEmpty.getShortObject(6));
        assertNull(kustoResultSetTableEmpty.getShortObject("g"));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getShort(6));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getShort("g"));

        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getByte(6));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getByte("g"));
    }

    @Test
    public void testKustoResultSetInteger_WhenHasValue_ReturnsValue() {
        assertEquals(INT_VAL, kustoResultSetTableWithValues.getIntegerObject(6));
        assertEquals(INT_VAL, kustoResultSetTableWithValues.getIntegerObject("g"));
        assertEquals(INT_VAL, kustoResultSetTableWithValues.getInt(6));
        assertEquals(INT_VAL, kustoResultSetTableWithValues.getInt("g"));

        assertEquals(INT_VAL, kustoResultSetTableWithValues.getLongObject(6));
        assertEquals(INT_VAL, kustoResultSetTableWithValues.getLongObject("g"));
        assertEquals(INT_VAL, kustoResultSetTableWithValues.getLong(6));
        assertEquals(INT_VAL, kustoResultSetTableWithValues.getLong("g"));

        assertEquals(Integer.valueOf(INT_VAL).shortValue(), kustoResultSetTableWithValues.getShortObject(6));
        assertEquals(Integer.valueOf(INT_VAL).shortValue(), kustoResultSetTableWithValues.getShortObject("g"));
        assertEquals(INT_VAL, kustoResultSetTableWithValues.getShort(6));
        assertEquals(INT_VAL, kustoResultSetTableWithValues.getShort("g"));

        assertEquals(INT_VAL, kustoResultSetTableWithValues.getByte(6));
        assertEquals(INT_VAL, kustoResultSetTableWithValues.getByte("g"));
    }

    @Test
    public void testKustoResultSetLong_WhenEmpty_ReturnsNullIfObjectOrThrows() {
        assertNull(kustoResultSetTableEmpty.getLongObject(7));
        assertNull(kustoResultSetTableEmpty.getLongObject("h"));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getLong(7));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getLong("h"));

        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getInt(7));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getInt("h"));
    }

    @Test
    public void testKustoResultSetLong_WhenHasValue_ReturnsValue() {
        assertEquals(LONG_VAL, kustoResultSetTableWithValues.getLongObject(7));
        assertEquals(LONG_VAL, kustoResultSetTableWithValues.getLong("h"));

        // Shouldn't be able to get a long as an int
        assertThrows(java.lang.ClassCastException.class, () -> kustoResultSetTableWithValues.getInt("h"));
    }

    @Test
    public void testKustoResultSetReal_WhenEmpty_ReturnsNullIfObjectOrThrows() {
        assertNull(kustoResultSetTableEmpty.getBigDecimal(8));
        assertNull(kustoResultSetTableEmpty.getBigDecimal("i"));

        assertNull(kustoResultSetTableEmpty.getDoubleObject(8));
        assertNull(kustoResultSetTableEmpty.getDoubleObject("i"));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getDouble(8));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getDouble("i"));

        assertNull(kustoResultSetTableEmpty.getFloatObject(8));
        assertNull(kustoResultSetTableEmpty.getFloatObject("i"));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getFloat(8));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getFloat("i"));
    }

    @Test
    public void testKustoResultSetReal_WhenHasValue_ReturnsValue() {
        assertEquals(BIGDECIMAL_VAL, kustoResultSetTableWithValues.getBigDecimal(8));
        assertEquals(BIGDECIMAL_VAL, kustoResultSetTableWithValues.getBigDecimal("i"));

        assertEquals(BIGDECIMAL_VAL.doubleValue(), kustoResultSetTableWithValues.getDoubleObject(8));
        assertEquals(BIGDECIMAL_VAL.doubleValue(), kustoResultSetTableWithValues.getDoubleObject("i"));
        assertEquals(BIGDECIMAL_VAL.doubleValue(), kustoResultSetTableWithValues.getDouble(8));
        assertEquals(BIGDECIMAL_VAL.doubleValue(), kustoResultSetTableWithValues.getDouble("i"));

        assertEquals(BIGDECIMAL_VAL.floatValue(), kustoResultSetTableWithValues.getFloatObject(8));
        assertEquals(BIGDECIMAL_VAL.floatValue(), kustoResultSetTableWithValues.getFloatObject("i"));
        assertEquals(BIGDECIMAL_VAL.floatValue(), kustoResultSetTableWithValues.getFloat(8));
        assertEquals(BIGDECIMAL_VAL.floatValue(), kustoResultSetTableWithValues.getFloat("i"));
    }

    @Test
    public void testKustoResultSetTimespan_WhenEmpty_ReturnsNull() throws SQLException {
        assertNull(kustoResultSetTableEmpty.getTime(9));
        assertNull(kustoResultSetTableEmpty.getTime("j"));
        assertNull(kustoResultSetTableEmpty.getLocalTime(9));
        assertNull(kustoResultSetTableEmpty.getLocalTime("j"));
    }

    @Test
    public void testKustoResultSetTimespan_WhenHasValue_ReturnsValue() throws SQLException {
        assertEquals(Time.valueOf(DURATION_AS_KUSTO_STRING_VAL), kustoResultSetTableWithValues.getTime(9));
        assertEquals(Time.valueOf(DURATION_AS_KUSTO_STRING_VAL), kustoResultSetTableWithValues.getTime("j"));

        assertEquals(LocalTime.parse(DURATION_AS_KUSTO_STRING_VAL), kustoResultSetTableWithValues.getLocalTime(9));
        assertEquals(LocalTime.parse(DURATION_AS_KUSTO_STRING_VAL), kustoResultSetTableWithValues.getLocalTime("j"));
    }

    @Test
    public void testKustoResultSetShort_WhenEmpty_ReturnsNullIfObjectOrThrows() {
        assertNull(kustoResultSetTableEmpty.getShortObject(10));
        assertNull(kustoResultSetTableEmpty.getShortObject("k"));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getShort(10));
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableEmpty.getShort("k"));
    }

    @Test
    public void testKustoResultSetShort_WhenHasValue_ReturnsValue() {
        assertEquals(SHORT_VAL, kustoResultSetTableWithValues.getShortObject(10));
        assertEquals(SHORT_VAL, kustoResultSetTableWithValues.getShortObject("k"));
        assertEquals(SHORT_VAL, kustoResultSetTableWithValues.getShort(10));
        assertEquals(SHORT_VAL, kustoResultSetTableWithValues.getShort("k"));
    }

    @Test
    // Also tests that when input table has a value that isn't in the mapping, we can reference its ordinal (as opposed to by column name, per the next test)
    public void testKustoResultSetByte_WhenHasValue_ReturnsValue() {
        assertEquals(BYTE_VAL, kustoResultSetTableWithValues.getByte(11));
    }

    @Test
    public void testKustoResultSetByte_WhenHasValueButNotInMappingAndReferenceByOrdinal_Throws() {
        assertThrows(java.lang.NullPointerException.class, () -> kustoResultSetTableWithValues.getByte("l"));
    }

    @Test
    public void testKustoResultSetFloat_WhenHasValue_ReturnsValue() {
        assertEquals(FLOAT_VAL, kustoResultSetTableWithValues.getFloatObject(12));
        assertEquals(FLOAT_VAL, kustoResultSetTableWithValues.getFloat(12));
    }

    @Test
    public void testKustoResultSetDouble_WhenHasValue_ReturnsValue() {
        assertEquals(BigDecimal.valueOf(DOUBLE_VAL), kustoResultSetTableWithValues.getBigDecimal(13));
    }

    @Test
    public void testJsonPropertyMissingException() {
        ObjectMapper objectMapper = Utils.getObjectMapper();
        ArrayNode rows = objectMapper.createArrayNode();

        String columns = "[ { \"ColumnType\": \"bool\" } ]";

        assertThrows(JsonPropertyMissingException.class, () -> new KustoResultSetTable(objectMapper.readTree("{\"TableName\":\"Table_0\"," +
                "\"Columns\":" + columns + ",\"Rows\":" +
                rows + "}")));
    }

    @Test
    public void testKustoServiceQueryError() {
        ObjectMapper objectMapper = Utils.getObjectMapper();
        ObjectNode row = objectMapper.createObjectNode();

        ArrayNode exceptionNode = objectMapper.createArrayNode();
        exceptionNode.add("Test exception");
        row.putIfAbsent(EXCEPTIONS_PROPERTY_NAME, exceptionNode);

        String columns = "[ { \"ColumnName\": \"a\", \"ColumnType\": \"bool\" } ]";

        KustoServiceQueryError thrownException = assertThrows(KustoServiceQueryError.class,
                () -> new KustoResultSetTable(objectMapper.readTree("{\"TableName\":\"Table_0\"," +
                        "\"Columns\":" + columns + ",\"Rows\":" +
                        objectMapper.createArrayNode().add(row) + "}")));
        assertEquals(1, thrownException.getExceptions().size());
        assertSame(thrownException.getExceptions().get(0).getClass(), Exception.class);
    }

    @Test
    public void testKustoServiceQueryErrors() {
        ObjectMapper objectMapper = Utils.getObjectMapper();
        ObjectNode row = objectMapper.createObjectNode();

        ArrayNode exceptionNode = objectMapper.createArrayNode();
        exceptionNode.add("Test exception1");
        exceptionNode.add("Test exception2");
        row.putIfAbsent(EXCEPTIONS_PROPERTY_NAME, exceptionNode);

        String columns = "[ { \"ColumnName\": \"a\", \"ColumnType\": \"bool\" } ]";

        KustoServiceQueryError thrownException = assertThrows(KustoServiceQueryError.class,
                () -> new KustoResultSetTable(objectMapper.readTree("{\"TableName\":\"Table_0\"," +
                        "\"Columns\":" + columns + ",\"Rows\":" +
                        objectMapper.createArrayNode().add(row) + "}")));
        assertEquals(2, thrownException.getExceptions().size());
        assertSame(thrownException.getExceptions().get(0).getClass(), Exception.class);
        assertSame(thrownException.getExceptions().get(1).getClass(), Exception.class);
    }
}
