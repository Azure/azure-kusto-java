// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.format.CslDateTimeFormat;
import com.microsoft.azure.kusto.data.format.CslTimespanFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.microsoft.azure.kusto.data.ClientRequestProperties.OPTION_SERVER_TIMEOUT;

class ClientRequestPropertiesTest {
    // Used by the 3 parameterized tests below, in the format: {inputTimespan, inputMillis, expectedTimespan, expectedMillis}. Each method uses only one of the
    // 2 input parameters.
    private static Stream<Arguments> provideInputsForConvertTimeoutBetweenMillisAndTimespan() {
        return Stream.of(
                Arguments.of("00:40:02", TimeUnit.MINUTES.toMillis(40) + TimeUnit.SECONDS.toMillis(2), "00:40:02",
                        TimeUnit.MINUTES.toMillis(40) + TimeUnit.SECONDS.toMillis(2)),
                // If set to over MAX_TIMEOUT_MS - value should be MAX_TIMEOUT_MS
                Arguments.of("01:40:02.1", TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(40) + TimeUnit.SECONDS.toMillis(2) + 100, "01:00:00",
                        ClientRequestProperties.MAX_TIMEOUT_MS),
                Arguments.of("1.01:40:02.1",
                        TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(40) + TimeUnit.SECONDS.toMillis(2) + 100, "01:00:00",
                        ClientRequestProperties.MAX_TIMEOUT_MS),
                // If set to under MIN_TIMEOUT_MS - value should be MIN_TIMEOUT_MS
                Arguments.of("00:00:12.6", TimeUnit.SECONDS.toMillis(12) + 600, "00:01:00", ClientRequestProperties.MIN_TIMEOUT_MS),
                Arguments.of("00:00:00", 0L, "00:01:00", ClientRequestProperties.MIN_TIMEOUT_MS),
                Arguments.of(null, null, null, null));
    }

    @ParameterizedTest
    @MethodSource("provideInputsForConvertTimeoutBetweenMillisAndTimespan")
    @DisplayName("Set the timeout in milliseconds, using helper method")
    void setTimeoutInMilliSec(String serverTimeoutOptionTimespan, Long serverTimeoutOptionMillis, String expectedTimespan, Long expectedMillis) {
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();

        // before setting value should be null
        Assertions.assertNull(clientRequestProperties.getTimeoutInMilliSec());
        Object timeoutObj = clientRequestProperties.getOption(OPTION_SERVER_TIMEOUT);
        Assertions.assertNull(clientRequestProperties.getTimeoutAsString(timeoutObj));

        clientRequestProperties.setTimeoutInMilliSec(serverTimeoutOptionMillis);
        Assertions.assertEquals(clientRequestProperties.getTimeoutInMilliSec(), expectedMillis);
        Assertions.assertEquals(clientRequestProperties.getOption(OPTION_SERVER_TIMEOUT), expectedMillis);
        Assertions.assertEquals(clientRequestProperties.getTimeoutAsCslTimespan(), expectedTimespan);
    }

    @ParameterizedTest
    @MethodSource("provideInputsForConvertTimeoutBetweenMillisAndTimespan")
    @DisplayName("Set the timeout in milliseconds, directly via OPTION_SERVER_TIMEOUT")
    void setOPTION_SERVER_TIMEOUTProvidingMillis(String serverTimeoutOptionTimespan, Long serverTimeoutOptionMillis, String expectedTimespan,
            Long expectedMillis) {
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();

        // before setting value should be null
        Assertions.assertNull(clientRequestProperties.getTimeoutInMilliSec());
        Object timeoutObj = clientRequestProperties.getOption(OPTION_SERVER_TIMEOUT);
        Assertions.assertNull(clientRequestProperties.getTimeoutAsString(timeoutObj));

        clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, serverTimeoutOptionMillis);
        Assertions.assertEquals(clientRequestProperties.getTimeoutInMilliSec(), expectedMillis);
        Assertions.assertEquals(clientRequestProperties.getTimeoutAsCslTimespan(), expectedTimespan);
    }

    @ParameterizedTest
    @MethodSource("provideInputsForConvertTimeoutBetweenMillisAndTimespan")
    @DisplayName("Set the timeout to a timespan, directly via OPTION_SERVER_TIMEOUT")
    void setOPTION_SERVER_TIMEOUTProvidingTimespan(String serverTimeoutOptionTimespan, Long serverTimeoutOptionMillis, String expectedTimespan,
            Long expectedMillis) {
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();

        // before setting value should be null
        Assertions.assertNull(clientRequestProperties.getTimeoutInMilliSec());
        Object timeoutObj = clientRequestProperties.getOption(OPTION_SERVER_TIMEOUT);
        Assertions.assertNull(clientRequestProperties.getTimeoutAsString(timeoutObj));

        clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, serverTimeoutOptionTimespan);
        Assertions.assertEquals(clientRequestProperties.getTimeoutInMilliSec(), expectedMillis);
        Assertions.assertEquals(clientRequestProperties.getTimeoutAsCslTimespan(), expectedTimespan);
    }

    @Test
    @DisplayName("test ClientRequestProperties toString")
    void propertiesToString() throws JsonProcessingException {
        ClientRequestProperties props = new ClientRequestProperties();
        props.setOption("a", 1);
        props.setOption("b", "hello");
        ObjectMapper objectMapper = Utils.getObjectMapper();

        Assertions.assertEquals(objectMapper.readTree("{\"Options\":{\"a\":1,\"b\":\"hello\"},\"Parameters\":{}}").toString(), props.toString());
    }

    @Test
    @DisplayName("test ClientRequestProperties fromString")
    void stringToProperties() throws JsonProcessingException {
        String properties = "{\"Options\":{\"servertimeout\":\"00:25:11.111\", \"Content-Encoding\":\"gzip\"},\"Parameters\":{\"birthday\":\"datetime(1970-05-11)\",\"courses\":\"dynamic(['Java', 'C++'])\"}}";
        ClientRequestProperties crp = ClientRequestProperties.fromString(properties);

        Assertions.assertNotNull(crp);
        Assertions.assertEquals("00:25:11.111", crp.toJson().get("Options").get(OPTION_SERVER_TIMEOUT).asText());
        Assertions.assertNotNull(crp.getTimeoutInMilliSec());
        Assertions.assertEquals("gzip", crp.getOption("Content-Encoding"));
        Assertions.assertEquals("datetime(1970-05-11)", crp.getParameter("birthday"));
        Assertions.assertEquals("dynamic(['Java', 'C++'])", crp.getParameter("courses"));

        Long timeoutMilli = 200000L;
        crp = new ClientRequestProperties();
        crp.setTimeoutInMilliSec(timeoutMilli);
        crp = ClientRequestProperties.fromString(crp.toString());
        Assertions.assertEquals(timeoutMilli, crp.getTimeoutInMilliSec());
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

    @ParameterizedTest
    @CsvSource({
            "2024-09-29T08:28:54.5440000Z, datetime(2024-09-29T08:28:54.5440000Z)",
            "datetime( 2024-09-29T08:28:54.5440000Z), datetime(2024-09-29T08:28:54.5440000Z)",
            "datetime( 2024), datetime(2024-01-01T00:00:00.0000000Z)",
            "h\\\"2024-09-29T08:28:54.5440000Z\\\", datetime(2024-09-29T08:28:54.5440000Z)",
    })
    void testCreateCslDateTimeFormat(String localDateTimeStringInput, String localDateTimeStringExpected) {
        String result = new CslDateTimeFormat(localDateTimeStringInput).toString();
        Assertions.assertEquals(localDateTimeStringExpected, result);
    }

    @Test
    void testCreateCslTimespanFormatFromString() {
        String timeString = "23:59:35.9853375";
        String result = new CslTimespanFormat(timeString).toString();
        Assertions.assertEquals("time(" + timeString + ")", result);
    }

    @Test
    void testRedirectCount() {
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        int redirectCount = clientRequestProperties.getRedirectCount();
        Assertions.assertEquals(0, redirectCount);

        clientRequestProperties.setOption(ClientRequestProperties.OPTION_CLIENT_MAX_REDIRECT_COUNT, 1);
        redirectCount = clientRequestProperties.getRedirectCount();
        Assertions.assertEquals(1, redirectCount);

        clientRequestProperties.setOption(ClientRequestProperties.OPTION_CLIENT_MAX_REDIRECT_COUNT, "1");
        redirectCount = clientRequestProperties.getRedirectCount();
        Assertions.assertEquals(1, redirectCount);
    }
}
