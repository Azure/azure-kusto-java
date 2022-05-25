package com.microsoft.azure.kusto.data;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class KustoDateTimeTest {
    @Test
    void KustoResultSet() throws Exception {
        JSONArray rows = new JSONArray();
        JSONArray row = new JSONArray();
        row.put(Instant.parse("2022-05-17T00:00:00Z"));
        rows.put(row);
        row = new JSONArray();
        row.put(Instant.parse("2022-05-17T00:00:00.1Z"));
        rows.put(row);
        row = new JSONArray();
        row.put(Instant.parse("2022-05-17T00:00:00.12Z"));
        rows.put(row);
        row = new JSONArray();
        row.put(Instant.parse("2022-05-17T00:00:00.123Z"));
        rows.put(row);
        row = new JSONArray();
        row.put(Instant.parse("2022-05-17T00:00:00.1234Z"));
        rows.put(row);
        row = new JSONArray();
        row.put(Instant.parse("2022-05-17T00:00:00.12345Z"));
        rows.put(row);
        row = new JSONArray();
        row.put(Instant.parse("2022-05-17T00:00:00.123456Z"));
        rows.put(row);
        row = new JSONArray();
        row.put(Instant.parse("2022-05-17T00:00:00.1234567Z"));
        rows.put(row);
        row = new JSONArray();
        row.put(Instant.parse("2022-05-17T00:00:00.12345678Z"));
        rows.put(row);
        row = new JSONArray();
        row.put(Instant.parse("2022-05-17T00:00:00.123456789Z"));
        rows.put(row);

        String columns = "[ { \"ColumnName\": \"a\", \"ColumnType\": \"datetime\" } ]";
        KustoResultSetTable res = new KustoResultSetTable(new JSONObject("{\"TableName\":\"Table_0\"," +
                "\"Columns\":" + columns + ",\"Rows\":" +
                rows.toString() + "}"));

        Integer rowNum = 0;                
        while (res.next())
        {
            Assertions.assertEquals(
                LocalDateTime.ofInstant((Instant)((JSONArray)rows.get(rowNum)).get(0), ZoneOffset.UTC), 
                res.getKustoDateTime(0));
            rowNum++;
        }
    }
}
