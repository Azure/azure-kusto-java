package com.microsoft.azure.kusto.data.format;

import com.microsoft.azure.kusto.data.Ensure;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Date;

public class CslDateTimeFormat extends CslFormat {
    public static final String KUSTO_DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'";
    public static final String KUSTO_DATETIME_PATTERN_NO_FRACTIONS = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(KUSTO_DATETIME_PATTERN).withZone(ZoneId.of("UTC"));

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
            DateTimeFormatter dateTimeFormatter;
            if (localDateTimeString.length() < 21) {
                dateTimeFormatter = new DateTimeFormatterBuilder().parseCaseInsensitive().append(DateTimeFormatter.ofPattern(KUSTO_DATETIME_PATTERN_NO_FRACTIONS)).toFormatter();
            } else {
                dateTimeFormatter = new DateTimeFormatterBuilder().parseCaseInsensitive().append(DateTimeFormatter.ofPattern(KUSTO_DATETIME_PATTERN)).toFormatter();
            }
            this.value = LocalDateTime.parse(localDateTimeString, dateTimeFormatter);
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

        return value.format(dtf);
    }
}