package com.microsoft.azure.kusto.data.format;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.exceptions.ParseException;

public class CslTimespanFormat extends CslFormat {
    public static final String KUSTO_TIMESPAN_PATTERN = "HH:mm:ss.SSSSSSS";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(KUSTO_TIMESPAN_PATTERN);

    private final Duration value;

    public CslTimespanFormat(Duration value) {
        this.value = value;
    }

    public CslTimespanFormat(String value) {
        if (StringUtils.isBlank(value)) {
            this.value = null;
        } else {
            Matcher matcher = ClientRequestProperties.KUSTO_TIMESPAN_REGEX.matcher(value);
            if (!matcher.matches()) {
                throw new ParseException(String.format("Failed to parse timeout string as a timespan. Value: %s", value));
            }

            long nanos = 0;
            String days = matcher.group(2);
            if (days != null && !days.equals("0")) {
                nanos = TimeUnit.DAYS.toNanos(Integer.parseInt(days));
            }

            String timespanWithoutDays = "";
            for (int i = 4; i <= 10; i++) {
                if (matcher.group(i) != null) {
                    timespanWithoutDays += matcher.group(i);
                }
            }
            nanos += LocalTime.parse(timespanWithoutDays).toNanoOfDay();
            if ("-".equals(matcher.group(1))) {
                this.value = Duration.ofNanos(nanos).negated();
            } else {
                this.value = Duration.ofNanos(nanos);
            }
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
        Duration valueWithoutDays = value.isNegative() ? value.negated() : value;
        if (valueWithoutDays.toDays() > 0) {
            result += valueWithoutDays.toDays() + ".";
            valueWithoutDays = valueWithoutDays.minusDays(valueWithoutDays.toDays());
        }
        result += LocalTime.MIDNIGHT.plus(valueWithoutDays).format(DATE_TIME_FORMATTER);
        if (value.isNegative()) {
            result = "-" + result;
        }

        return result;
    }
}
