package com.microsoft.azure.kusto.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import com.microsoft.azure.kusto.data.http.HttpPostUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.UUID;

public class ResultSetTest {
    @Test
    void KustoResultSet() throws Exception {
        ObjectMapper objectMapper = Utils.getObjectMapper();

        ArrayNode rows = objectMapper.createArrayNode();
        ArrayNode row1 = objectMapper.createArrayNode();
        ArrayNode row2 = objectMapper.createArrayNode();

        JsonNode nullObj = null;
        row2.add(nullObj);
        row2.add("");
        row2.add(nullObj);
        row2.add(nullObj);
        row2.add(nullObj);
        row2.add(nullObj);
        row2.add(nullObj);
        row2.add(nullObj);
        row2.add(nullObj);
        row2.add(nullObj);
        row2.add(nullObj);
        rows.add(row2);

        String str = "str";
        Instant now = Instant.now();

        Duration duration = Duration.ofHours(2).plusSeconds(1);
        BigDecimal dec = BigDecimal.valueOf(1, 1);
        UUID uuid = UUID.randomUUID();
        int i = 1;
        long l = 100000000000L;
        double d = 1.1d;
        short s1 = 10;
        float f1 = 15.0f;
        BigDecimal bigDecimal = new BigDecimal("10.0003214134245341414141314134134101");
        String durationAsKustoString = LocalTime.MIDNIGHT.plus(duration).toString();
        row1.add(true);
        row1.add(str);
        row1.add(String.valueOf(now));
        row1.add(dec);
        row1.add(objectMapper.createObjectNode());
        row1.add(String.valueOf(uuid));
        row1.add(i);
        row1.add(l);
        row1.add(bigDecimal);
        row1.add(durationAsKustoString);
        row1.add(s1);
        row1.add(f1);
        row1.add(d);
        rows.add(row1);

        String columns = "[ { \"ColumnName\": \"a\", \"ColumnType\": \"bool\" }, { \"ColumnName\": \"b\", " +
                "\"ColumnType\": \"string\" }, { \"ColumnName\": \"c\", \"ColumnType\": \"datetime\" }, { " +
                "\"ColumnName\": \"d\", \"ColumnType\": \"decimal\" }, { \"ColumnName\": \"e\", " +
                "\"ColumnType\": \"dynamic\" }, { \"ColumnName\": \"f\", \"ColumnType\": \"guid\" }, { " +
                "\"ColumnName\": \"g\", \"ColumnType\": \"int\" }, { \"ColumnName\": \"h\", \"ColumnType\": " +
                "\"long\" }, { \"ColumnName\": \"i\", \"ColumnType\": \"real\" }, { \"ColumnName\": \"j\", " +
                "\"ColumnType\": \"timespan\" },{ \"ColumnName\": \"k\", \"ColumnType\": \"short\" } ]";
        KustoResultSetTable res = new KustoResultSetTable(objectMapper.readTree("{\"TableName\":\"Table_0\"," +
                "\"Columns\":" + columns + ",\"Rows\":" +
                rows + "}"));
        res.next();
        assert res.getBooleanObject(0) == null;
        assert res.getString(1).equals("");
        assert res.getTimestamp(2) == null;
        assert res.getKustoDateTime(2) == null;
        assert res.getBigDecimal(3) == null;
        assert res.getJSONObject(4) == null;
        assert res.getUUID(5) == null;
        assert res.getIntegerObject(6) == null;
        assert res.getLongObject(7) == null;
        assert res.getDoubleObject(8) == null;
        assert res.getTime(9) == null;
        assert res.getShortObject(10) == null;

        res.next();
        Assertions.assertTrue(res.getBooleanObject(0));
        Assertions.assertEquals(res.getString(1), str);
        Assertions.assertEquals(res.getTimestamp(2), Timestamp.valueOf(now.atZone(ZoneId.of("UTC")).toLocalDateTime()));
        Assertions.assertEquals(LocalDateTime.ofInstant(now, ZoneOffset.UTC), res.getKustoDateTime(2));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Assertions.assertEquals(sdf.format(new Date(now.getEpochSecond() * 1000)), res.getDate(2).toString());

        Assertions.assertEquals(res.getBigDecimal(3), dec);
        Assertions.assertEquals(res.getJSONObject(4), objectMapper.createObjectNode());
        Assertions.assertEquals(res.getUUID(5), uuid);
        Assertions.assertEquals(res.getIntegerObject(6), i);
        Assertions.assertEquals(res.getLongObject(7), l);
        Assertions.assertEquals(res.getBigDecimal(8), bigDecimal);

        Assertions.assertEquals(res.getTime(9), Time.valueOf(durationAsKustoString));
        Assertions.assertEquals(res.getLocalTime(9), LocalTime.parse(durationAsKustoString));
        Assertions.assertEquals(res.getShort(10), s1);
        Assertions.assertEquals(res.getBigDecimal(11), BigDecimal.valueOf(f1));
        Assertions.assertEquals(res.getBigDecimal(12), BigDecimal.valueOf(d));

    }

    @Test
    public void testException() {
        ObjectMapper objectMapper = Utils.getObjectMapper();

        ArrayNode rows = objectMapper.createArrayNode();
        ObjectNode row = objectMapper.createObjectNode();

        ArrayNode exceptionNode = objectMapper.createArrayNode();
        exceptionNode.add("Test exception");
        row.putIfAbsent("Exceptions", exceptionNode);

        rows.add(row);

        String columns = "[ { \"ColumnName\": \"a\", \"ColumnType\": \"bool\" }, { \"ColumnName\": \"b\", " +
                "\"ColumnType\": \"string\" }, { \"ColumnName\": \"c\", \"ColumnType\": \"datetime\" }, { " +
                "\"ColumnName\": \"d\", \"ColumnType\": \"decimal\" }, { \"ColumnName\": \"e\", " +
                "\"ColumnType\": \"dynamic\" }, { \"ColumnName\": \"f\", \"ColumnType\": \"guid\" }, { " +
                "\"ColumnName\": \"g\", \"ColumnType\": \"int\" }, { \"ColumnName\": \"h\", \"ColumnType\": " +
                "\"long\" }, { \"ColumnName\": \"i\", \"ColumnType\": \"real\" }, { \"ColumnName\": \"j\", " +
                "\"ColumnType\": \"timespan\" },{ \"ColumnName\": \"k\", \"ColumnType\": \"short\" } ]";

        Assertions.assertThrows(KustoServiceQueryError.class, () -> {
            new KustoResultSetTable(objectMapper.readTree("{\"TableName\":\"Table_0\"," +
                    "\"Columns\":" + columns + ",\"Rows\":" +
                    rows + "}"));
        });

    }
}
