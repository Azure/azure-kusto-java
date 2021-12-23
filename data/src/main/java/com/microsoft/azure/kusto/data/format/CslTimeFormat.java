package com.microsoft.azure.kusto.data.format;

import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.Ensure;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.ParseException;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

public class CslTimeFormat extends CslFormat {
    public static final String KUSTO_TIME_PATTERN = "HH:mm:ss.SSSSSSS";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(KUSTO_TIME_PATTERN);

    private final Duration value;

    public CslTimeFormat(Duration value) {
        this.value = value;
    }

    public CslTimeFormat(String value) {
        if (StringUtils.isBlank(value)) {
            this.value = null;
        } else {
            Matcher matcher = ClientRequestProperties.PATTERN.matcher(value);
            if (!matcher.matches()) {
                throw new ParseException(String.format("Failed to parse timeout string as a timespan. Value: %s", value));
            }

            long nanos = 0;
            String days = matcher.group(1);
            if (days != null && !days.equals("0")) {
                nanos = TimeUnit.DAYS.toNanos(Integer.parseInt(days));
            }

            String timespanWithoutDays = "";
            for (int i = 3; i <= 9; i++) {
                if (matcher.group(i) != null) {
                    timespanWithoutDays += matcher.group(i);
                }
            }
            nanos += LocalTime.parse(timespanWithoutDays).toNanoOfDay();
            this.value = Duration.ofNanos(nanos);
        }
    }

    @Override
    public String getType() {
        return "time";
    }

    @Override
    public Duration getValue() {
        return value;
    }

    @Override
    String getValueAsString() {
        Ensure.argIsNotNull(value, "value");

        String result = "";
        Duration valueWithoutDays = value;
        if (value.toDays() > 0) {
            result += value.toDays() + ".";
            valueWithoutDays = value.minusDays(value.toDays());
        }
        result += LocalTime.MIDNIGHT.plus(valueWithoutDays).format(DATE_TIME_FORMATTER);
        return result;
    }
}