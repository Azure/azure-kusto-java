package com.microsoft.azure.kusto.data;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
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
        rows.put(row2);

        String str = "str";
        Instant now = Instant.now();
        Duration duration = Duration.ofHours(2);
        BigDecimal dec = BigDecimal.valueOf(1, 1);
        UUID uuid = UUID.randomUUID();
        int i = 1;
        long l = 100000000000L;
        double d = 1.1d;

        row1.put(true);
        row1.put(str);
        row1.put(now);
        row1.put(dec);
        row1.put(new Object());
        row1.put(uuid);
        row1.put(i);
        row1.put(l);
        row1.put(d);
        row1.put(duration);
        rows.put(row1);

        String columns = "[ { \"ColumnName\": \"a\", \"ColumnType\": \"bool\" }, { \"ColumnName\": \"b\", " +
                "\"ColumnType\": \"string\" }, { \"ColumnName\": \"c\", \"ColumnType\": \"datetime\" }, { " +
                "\"ColumnName\": \"d\", \"ColumnType\": \"decimal\" }, { \"ColumnName\": \"e\", " +
                "\"ColumnType\": \"dynamic\" }, { \"ColumnName\": \"f\", \"ColumnType\": \"guid\" }, { " +
                "\"ColumnName\": \"g\", \"ColumnType\": \"int\" }, { \"ColumnName\": \"h\", \"ColumnType\": " +
                "\"long\" }, { \"ColumnName\": \"i\", \"ColumnType\": \"real\" }, { \"ColumnName\": \"j\", " +
                "\"ColumnType\": \"timespan\" } ]";
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
        assert res.getInteger(6) == null;
        assert res.getLongObject(7) == null;
        assert res.getDoubleObject(8) == null;
        assert res.getTime(9) == null;

        res.next();
        assert res.getBooleanObject(0);
        assert res.getString(1).equals(str);
        assert res.getTimestamp(2).equals(Timestamp.from(now));
        assert res.getBigDecimal(3).equals(dec);
        assert res.getJSONObject(4).equals(new JSONObject());
        assert res.getUUID(5) == uuid;
        assert res.getInteger(6) == i;
        assert res.getLongObject(7) == l;
        assert res.getDoubleObject(8) == d;
        assert res.getTime(9).equals(Time.valueOf(duration.toString()));
    }
}
