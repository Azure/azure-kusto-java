package com.microsoft.azure.kusto.data.format;

import com.microsoft.azure.kusto.data.Ensure;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CslDateTimeFormat extends CslFormat {
    public static final String KUSTO_DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'";
    public static final String KUSTO_DATETIME_PATTERN_NO_FRACTIONS = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(KUSTO_DATETIME_PATTERN).withZone(ZoneId.of("UTC"));
    Map<Integer, String> KUSTO_DATETIME_FORMATS = Stream
            .of(
                    new Object[][] {
                            {4, "yyyy"},
                            {6, "yyyyMM"},
                            {8, "yyyyMMdd"},
                            {10, "yyyyMMddHH"},
                            {12, "yyyyMMddHHmm"},
                            {14, "yyyyMMddHHmmss"},
                            {17, "yyyyMMdd HH:mm:ss"},
                            {19, "yyyyMMdd HH:mm:ss.f"},
                            {20, "yyyyMMdd HH:mm:ss.ff"},
                            {21, "yyyyMMdd HH:mm:ss.fff"},
                            {22, "yyyyMMdd HH:mm:ss.ffff"},
                            {23, "yyyyMMdd HH:mm:ss.fffff"},
                            {24, "yyyyMMdd HH:mm:ss.ffffff"},
                            {25, "yyyyMMdd HH:mm:ss.fffffff"}
                    })
            .collect(Collectors.toMap(data -> (Integer) data[0], data -> (String) data[1]));

    private final LocalDateTime value;

    public CslDateTimeFormat(LocalDateTime date) {
        this.value = date;
    }

    public CslDateTimeFormat(Date date) {
        if (date == null) {
            this.value = null;
        } else {
            this.value = date.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime();
        }
    }

    public CslDateTimeFormat(Long date) {
        this(date != null ? new Date(date) : null);
    }

    public CslDateTimeFormat(String localDateTimeString) {
        if (StringUtils.isBlank(localDateTimeString)) {
            this.value = null;
        } else {
            localDateTimeString = parseValueFromValueWithType(localDateTimeString, getType()).trim();
            localDateTimeString = CslStringFormat.parseStringLiteral(localDateTimeString).trim();

            if ("null".equals(localDateTimeString)) {
                value = null;
            } else if ("min".equals(localDateTimeString)) {
                value = new Date(0L).toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime();
            } else if ("max".equals(localDateTimeString)) {
                value = LocalDateTime.MAX;
            } else if ("now".equalsIgnoreCase(localDateTimeString)) {
                value = Instant.now().atZone(ZoneId.of("UTC")).toLocalDateTime();
            } else {
                value = parseDateTimeFromString(localDateTimeString);
            }
        }
    }

    @Override
    public String getType() {
        return "datetime";
    }

    @Override
    public LocalDateTime getValue() {
        return value;
    }

    @Override
    String getValueAsString() {
        Ensure.argIsNotNull(value, "value");

        return value.format(DATE_TIME_FORMATTER);
    }

    private LocalDateTime parseDateTimeFromString(String localDateTimeString) {
        DateTimeFormatter dateTimeFormatter;

        // Try to parse using well-known Kusto masks
        String formatMask = KUSTO_DATETIME_FORMATS.get(localDateTimeString.length());
        if (formatMask != null) {
            try {
                dateTimeFormatter = new DateTimeFormatterBuilder()
                        .appendPattern(formatMask)
                        .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                        .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                        .toFormatter();
                return LocalDateTime.parse(localDateTimeString, dateTimeFormatter);
            } catch (Exception e) {
            }
        }

        // Try to parse ISO 8601 string without providing mask
        try {
            TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(localDateTimeString);
            Instant i = Instant.from(ta);
            return Date.from(i).toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime();
        } catch (Exception e) {
        }

        // Old Java SDK approach to parsing, using particular mask
        try {
            if (localDateTimeString.length() < 21) {
                dateTimeFormatter = new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(DateTimeFormatter.ofPattern(KUSTO_DATETIME_PATTERN_NO_FRACTIONS))
                        .toFormatter();
            } else {
                dateTimeFormatter = new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(DateTimeFormatter.ofPattern(KUSTO_DATETIME_PATTERN))
                        .toFormatter();
            }
            return LocalDateTime.parse(localDateTimeString, dateTimeFormatter);
        } catch (Exception e) {
            return null;
        }
    }
}
