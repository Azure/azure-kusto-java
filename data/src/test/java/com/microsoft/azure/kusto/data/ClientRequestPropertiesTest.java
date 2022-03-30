// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.format.CslDateTimeFormat;
import com.microsoft.azure.kusto.data.format.CslTimespanFormat;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

class ClientRequestPropertiesTest {
    @Test
    @DisplayName("test set/get timeout")
    void timeoutSetGet() {
        ClientRequestProperties props = new ClientRequestProperties();
        Long expected = TimeUnit.MINUTES.toMillis(100);

        // before setting value should be null
        Assertions.assertNull(props.getTimeoutInMilliSec());

        props.setTimeoutInMilliSec(expected);
        Assertions.assertEquals(expected, props.getTimeoutInMilliSec());
    }

    @Test
    @DisplayName("test ClientRequestProperties toString")
    void propertiesToString() throws JSONException {
        ClientRequestProperties props = new ClientRequestProperties();
        props.setOption("a", 1);
        props.setOption("b", "hello");

        JSONAssert.assertEquals("{\"Options\": {\"a\":1, \"b\":\"hello\"}}", props.toString(), false);
    }

    @Test
    @DisplayName("test ClientRequestProperties fromString")
    void stringToProperties() throws JSONException {
        String properties = "{\"Options\":{\"servertimeout\":\"01:25:11.111\", \"Content-Encoding\":\"gzip\"},\"Parameters\":{\"birthday\":\"datetime(1970-05-11)\",\"courses\":\"dynamic(['Java', 'C++'])\"}}";
        ClientRequestProperties crp = ClientRequestProperties.fromString(properties);
        assert crp != null;
        assert crp.toJson().getJSONObject("Options").get("servertimeout").equals("01:25:11.111");
        assert crp.getTimeoutInMilliSec() != null;
        assert crp.getOption("Content-Encoding").equals("gzip");
        assert crp.getParameter("birthday").equals("datetime(1970-05-11)");
        assert crp.getParameter("courses").equals("dynamic(['Java', 'C++'])");
    }

    @Test
    void testSetPropertyWithStringCslFormat() {
        String paramName = "stringParamName";
        String testStr = "testStr";

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter(paramName, testStr);

        Assertions.assertEquals(testStr, crp.getParameter(paramName));
    }

    @Test
    void testSetPropertyWithDateCslFormat() {
        String paramName = "dateParamName";
        Date testDate = new Date(1727598534544L);

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter(paramName, testDate);

        Assertions.assertEquals("datetime(2024-09-29T08:28:54.5440000Z)", crp.getParameter(paramName));
    }

    @Test
    void testSetPropertyWithLocalDateTimeCslFormat() {
        String paramName = "localDateTimeParamName";
        Date testDate = new Date(1727598534544L);
        LocalDateTime testLocalDateTime = LocalDateTime.ofInstant(testDate.toInstant(), ZoneId.of("UTC"));

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter(paramName, testLocalDateTime);

        Assertions.assertEquals("datetime(2024-09-29T08:28:54.5440000Z)", crp.getParameter(paramName));
    }

    @Test
    void testSetPropertyWithDurationCslFormat() {
        String paramName = "localDurationParamName";
        Duration testDuration = Duration.ofNanos(86375985337544L);

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter(paramName, testDuration);

        Assertions.assertEquals("time(23:59:35.9853375)", crp.getParameter(paramName));
    }

    @Test
    void testSetPropertyWithDurationWithDaysCslFormat() {
        String paramName = "localDurationWithDaysParamName";
        Duration testDurationWithDays = Duration.ofNanos(96375985337544L);

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter(paramName, testDurationWithDays);

        Assertions.assertEquals("time(1.02:46:15.9853375)", crp.getParameter(paramName));
    }

    @Test
    void testSetPropertyWithBooleanCslFormat() {
        String paramName = "boolParamName";
        boolean testBool = true;

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter(paramName, testBool);

        Assertions.assertEquals("bool(true)", crp.getParameter(paramName));
    }

    @Test
    void testSetPropertyWithIntCslFormat() {
        String paramName = "intParamName";
        int testInt = 4;

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter(paramName, testInt);

        Assertions.assertEquals("int(4)", crp.getParameter(paramName));
    }

    @Test
    void testSetPropertyWithLongCslFormat() {
        String paramName = "longParamName";
        long testLong = 1044L;

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter(paramName, testLong);

        Assertions.assertEquals("long(1044)", crp.getParameter(paramName));
    }

    @Test
    void testSetPropertyWithDoubleCslFormat() {
        String paramName = "doubleParamName";
        double testDouble = 32.3;

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter(paramName, testDouble);

        Assertions.assertEquals("real(32.3)", crp.getParameter(paramName));
    }

    @Test
    void testSetPropertyWithUuidCslFormat() {
        String paramName = "uuidParamName";
        UUID testUuid = UUID.randomUUID();

        ClientRequestProperties crp = new ClientRequestProperties();
        crp.setParameter(paramName, testUuid);

        Assertions.assertEquals("guid(" + testUuid + ")", crp.getParameter(paramName));
    }

    @Test
    void testCreateCslDateTimeFormatFromString() {
        String localDateTimeString = "2024-09-29T08:28:54.5440000Z";
        String result = new CslDateTimeFormat(localDateTimeString).toString();
        Assertions.assertEquals("datetime(" + localDateTimeString + ")", result);
    }

    @Test
    void testCreateCslDateTimeFormatFromStringWithTypeAndValue() {
        String localDateTimeString = "datetime( 2024-09-29T08:28:54.5440000Z)";
        String result = new CslDateTimeFormat(localDateTimeString).toString();
        Assertions.assertEquals("datetime(2024-09-29T08:28:54.5440000Z)", result);
    }

    @Test
    void testCreateCslDateTimeFormatFromStringWithJustYear() {
        String localDateTimeString = "datetime( 2024)";
        String result = new CslDateTimeFormat(localDateTimeString).toString();
        Assertions.assertEquals("datetime(2024-01-01T00:00:00.0000000Z)", result);
    }

    @Test
    void testCreateCslDateTimeFormatFromStringWithStringLiteral() {
        String localDateTimeString = "h\\\"2024-09-29T08:28:54.5440000Z\\\"";
        String result = new CslDateTimeFormat(localDateTimeString).toString();
        Assertions.assertEquals("datetime(2024-09-29T08:28:54.5440000Z)", result);
    }

    @Test
    void testCreateCslTimespanFormatFromString() {
        String timeString = "23:59:35.9853375";
        String result = new CslTimespanFormat(timeString).toString();
        Assertions.assertEquals("time(" + timeString + ")", result);
    }
}