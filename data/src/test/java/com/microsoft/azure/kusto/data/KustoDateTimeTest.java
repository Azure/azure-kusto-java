package com.microsoft.azure.kusto.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

class KustoDateTimeTest {
    @Test
    void kustoResultSet() throws Exception {
        ObjectMapper mapper = Utils.getObjectMapper();
        DateTimeFormatter kustoDateTimeFormatter = new DateTimeFormatterBuilder().parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME).appendLiteral('Z').toFormatter();
        ArrayNode rows = mapper.createArrayNode();
        ArrayNode row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00Z").toString());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.1Z").toString());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.12Z").toString());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.123Z").toString());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.1234Z").toString());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.12345Z").toString());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.123456Z").toString());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.1234567Z").toString());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.12345678Z").toString());
        rows.add(row);
        row = mapper.createArrayNode();
        row.add(Instant.parse("2022-05-17T00:00:00.123456789Z").toString());
        rows.add(row);

        String columns = "[ { \"ColumnName\": \"a\", \"ColumnType\": \"datetime\" } ]";
        KustoResultSetTable res = new KustoResultSetTable(mapper.createArrayNode().add("{\"TableName\":\"Table_0\"," +
                "\"Columns\":" + columns + ",\"Rows\":" +
                rows + "}"));

        int rowNum = 0;
        while (res.next()) {
            Assertions.assertEquals(
                    LocalDateTime.parse(rows.get(rowNum).get(0).toString(), kustoDateTimeFormatter),
                    res.getKustoDateTime(0));
            rowNum++;
        }
    }
}
