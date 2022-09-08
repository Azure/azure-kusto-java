package com.microsoft.azure.kusto.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

public class KustoDateTimeTest {
    @Test
    void KustoResultSet() throws Exception {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode rows = mapper.createArrayNode();
        ArrayNode row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00Z").getNano());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.1Z").getNano());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.12Z").getNano());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.123Z").getNano());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.1234Z").getNano());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.12345Z").getNano());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.123456Z").getNano());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.1234567Z").getNano());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.12345678Z").getNano());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.123456789Z").getNano());
        rows.add(row);

        String columns = "[ { \"ColumnName\": \"a\", \"ColumnType\": \"datetime\" } ]";
        KustoResultSetTable res = new KustoResultSetTable(mapper.createArrayNode().add("{\"TableName\":\"Table_0\"," +
                "\"Columns\":" + columns + ",\"Rows\":" +
                rows + "}"));

        Integer rowNum = 0;
        while (res.next()) {
            Assertions.assertEquals(
                    LocalDateTime.parse(rows.get(rowNum).get(0).toString(), dateTimeFormatter).getNano(),
                    res.getKustoDateTime(0).getNano());
            rowNum++;
        }
    }
}
