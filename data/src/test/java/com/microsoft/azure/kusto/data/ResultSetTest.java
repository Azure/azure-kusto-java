package com.microsoft.azure.kusto.data;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.UUID;

public class ResultSetTest {
    @Test
    void KustoResultSet() throws Exception {
        JSONArray rows = new JSONArray();
        JSONArray row1 = new JSONArray();
        JSONArray row2 = new JSONArray();
        JSONObject nullObj = null;
        row2.put(nullObj);
        row2.put("");
        row2.put(nullObj);
        row2.put(nullObj);
        row2.put(nullObj);
        row2.put(nullObj);
        row2.put(nullObj);
        row2.put(nullObj);
        row2.put(nullObj);
        row2.put(nullObj);
        row2.put(nullObj);
        rows.put(row2);

        String str = "str";
        Instant now = Instant.now();

        Duration duration = Duration.ofHours(2).plusSeconds(1);
        BigDecimal dec = BigDecimal.valueOf(1, 1);
        UUID uuid = UUID.randomUUID();
        int i = 1;
        long l = 100000000000L;
        double d = 1.1d;
        short s1 = 10;
        String durationAsKustoString = LocalTime.MIDNIGHT.plus(duration).toString();
        row1.put(true);
        row1.put(str);
        row1.put(now);
        row1.put(dec);
        row1.put(new JSONObject());
        row1.put(uuid);
        row1.put(i);
        row1.put(l);
        row1.put(BigDecimal.valueOf(d));
        row1.put(durationAsKustoString);
        row1.put(s1);
        rows.put(row1);

        String columns = "[ { \"ColumnName\": \"a\", \"ColumnType\": \"bool\" }, { \"ColumnName\": \"b\", " +
                "\"ColumnType\": \"string\" }, { \"ColumnName\": \"c\", \"ColumnType\": \"datetime\" }, { " +
                "\"ColumnName\": \"d\", \"ColumnType\": \"decimal\" }, { \"ColumnName\": \"e\", " +
                "\"ColumnType\": \"dynamic\" }, { \"ColumnName\": \"f\", \"ColumnType\": \"guid\" }, { " +
                "\"ColumnName\": \"g\", \"ColumnType\": \"int\" }, { \"ColumnName\": \"h\", \"ColumnType\": " +
                "\"long\" }, { \"ColumnName\": \"i\", \"ColumnType\": \"real\" }, { \"ColumnName\": \"j\", " +
                "\"ColumnType\": \"timespan\" },{ \"ColumnName\": \"k\", \"ColumnType\": \"short\" } ]";
        KustoResultSetTable res = new KustoResultSetTable(new JSONObject("{\"TableName\":\"Table_0\"," +
                "\"Columns\":" + columns + ",\"Rows\":" +
                rows.toString() + "}"));
        res.next();
        assert res.getBooleanObject(0) == null;
        assert res.getString(1).equals("");
        assert res.getTimestamp(2) == null;
        assert res.getBigDecimal(3) == null;
        assert res.getJSONObject(4) == null;
        assert res.getUUID(5) == null;
        assert res.getIntegerObject(6) == null;
        assert res.getLongObject(7) == null;
        assert res.getDoubleObject(8) == null;
        assert res.getTime(9) == null;
        assert res.getShortObject(9) == null;

        res.next();
        Assertions.assertTrue(res.getBooleanObject(0));
        Assertions.assertEquals(res.getString(1),str);
        Assertions.assertEquals(res.getTimestamp(2), Timestamp.valueOf(now.atZone(ZoneId.of("UTC")).toLocalDateTime()));
        Assertions.assertEquals(new Date(now.getEpochSecond() * 1000).toString(), res.getDate(2).toString());

        Assertions.assertEquals(res.getBigDecimal(3), dec);
        Assertions.assertEquals(res.getJSONObject(4).toString(), new JSONObject().toString());
        Assertions.assertEquals(res.getUUID(5), uuid);
        Assertions.assertEquals(res.getIntegerObject(6), i);
        Assertions.assertEquals(res.getLongObject(7), l);
        Assertions.assertEquals(res.getDoubleObject(8),d);

        Assertions.assertEquals(res.getTime(9), Time.valueOf(durationAsKustoString));
        Assertions.assertEquals(res.getLocalTime(9), LocalTime.parse(durationAsKustoString));
        Assertions.assertEquals(res.getShort(10), s1);
    }
}
