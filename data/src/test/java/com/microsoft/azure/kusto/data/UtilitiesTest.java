package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;

import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.HttpStatus;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

class UtilitiesTest {
    @Test
    @DisplayName("Convert millis to .Net timespan")
    void convertMillisToTimespan() {
        Long timeout = TimeUnit.MINUTES.toMillis(40) + TimeUnit.SECONDS.toMillis(2); // 40 minutes 2 seconds
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        clientRequestProperties.setTimeoutInMilliSec(timeout);
        Assertions.assertEquals(timeout, clientRequestProperties.getTimeoutInMilliSec());
        Assertions.assertEquals(timeout, clientRequestProperties.getOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT));

        String serverTimeoutOptionStr = "01:40:02.1";
        long serverTimeoutOptionMillis = TimeUnit.HOURS.toMillis(1)
                + TimeUnit.MINUTES.toMillis(40)
                + TimeUnit.SECONDS.toMillis(2) + 100L;
        clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, serverTimeoutOptionStr);
        Assertions.assertEquals(serverTimeoutOptionMillis, clientRequestProperties.getTimeoutInMilliSec());

        // If set to over MAX_TIMEOUT_MS - value should be MAX_TIMEOUT_MS
        clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, "1.01:40:02.1");
        Assertions.assertEquals(ClientRequestProperties.MAX_TIMEOUT_MS,
                clientRequestProperties.getTimeoutInMilliSec());

        clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, "15:00");
        Assertions.assertEquals(TimeUnit.HOURS.toMillis(15), clientRequestProperties.getTimeoutInMilliSec());
    }

    @Test
    @DisplayName("Test exception creation when the web response is null")
    void createExceptionFromResponseNoResponse() {
        DataServiceException error = Utils.createExceptionFromResponse("https://sample.kusto.windows.net", null, new Exception(), "error");
        Assertions.assertEquals("POST failed to send request", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
    }

    @Test
    @DisplayName("Test exception creation on a 404 error")
    void createExceptionFromResponse404Error() {
        BasicHttpResponse basicHttpResponse = getBasicHttpResponse(404);
        DataServiceException error = Utils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(), "error");
        Assertions.assertTrue(error.getStatusCode() != null && error.getStatusCode() == HttpStatus.SC_NOT_FOUND);
    }

    @Test
    @DisplayName("Test exception creation from an oneapi error")
    void createExceptionFromResponseOneApi() {
        String OneApiError = "{\"error\": {\n" +
                "                \"code\": \"LimitsExceeded\",\n" +
                "                \"message\": \"Request is invalid and cannot be executed.\",\n" +
                "                \"@type\": \"Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException\",\n" +
                "                \"@message\": \"Query execution has exceeded the allowed limits (80DA0003): .\",\n" +
                "                \"@context\": {\n" +
                "                    \"timestamp\": \"2018-12-10T15:10:48.8352222Z\",\n" +
                "                    \"machineName\": \"RD0003FFBEDEB9\",\n" +
                "                    \"processName\": \"Kusto.Azure.Svc\",\n" +
                "                    \"processId\": 4328,\n" +
                "                    \"threadId\": 7284,\n" +
                "                    \"appDomainName\": \"RdRuntime\",\n" +
                "                    \"clientRequestd\": \"KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3\",\n" +
                "                    \"activityId\": \"a57ec272-8846-49e6-b458-460b841ed47d\",\n" +
                "                    \"subActivityId\": \"a57ec272-8846-49e6-b458-460b841ed47d\",\n" +
                "                    \"activityType\": \"PO-OWIN-CallContext\",\n" +
                "                    \"parentActivityId\": \"a57ec272-8846-49e6-b458-460b841ed47d\",\n" +
                "                    \"activityStack\": \"(Activity stack: CRID=KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3 ARID=a57ec272-8846-49e6-b458-460b841ed47d > PO-OWIN-CallContext/a57ec272-8846-49e6-b458-460b841ed47d)\"\n"
                +
                "                },\n" +
                "                \"@permanent\": true\n" +
                "            }}";
        BasicHttpResponse basicHttpResponse = getBasicHttpResponse(401);
        DataServiceException error = Utils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(), OneApiError);
        Assertions.assertEquals("Query execution has exceeded the allowed limits (80DA0003): ., ActivityId='1234'", error.getMessage());
        Assertions.assertTrue(error.getCause() instanceof DataWebException);
        Assertions.assertTrue(error.isPermanent());
        Assertions.assertEquals(401, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from a message object")
    void createExceptionFromMessageError() {
        String errorMessage = "{\"message\": \"Test Error Message\"}";
        BasicHttpResponse basicHttpResponse = getBasicHttpResponse(401);
        DataServiceException error = Utils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(), errorMessage);
        Assertions.assertEquals("Test Error Message, ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(401, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from a bad json")
    void createExceptionFromBadJson() {
        String errorMessage = "\"message\": \"Test Error Message\"";
        BasicHttpResponse basicHttpResponse = getBasicHttpResponse(401);
        DataServiceException error = Utils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(), errorMessage);
        Assertions.assertEquals("\"message\": \"Test Error Message\", ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(401, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from an unexpected json")
    void createExceptionFromOtherJson() {
        String errorMessage = "{\"response\": \"Test Error Message\"}";
        BasicHttpResponse basicHttpResponse = getBasicHttpResponse(401);
        DataServiceException error = Utils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(), errorMessage);
        Assertions.assertEquals("{\"response\": \"Test Error Message\"}, ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(401, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from a blank error message")
    void createExceptionFromBlankErrorMessage() {
        String errorMessage = " ";
        BasicHttpResponse basicHttpResponse = getBasicHttpResponse(401);
        DataServiceException error = Utils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(), errorMessage);
        Assertions.assertEquals("Http StatusCode='http/1.1 401 Some Error', ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(401, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Remove extension")
    void removeExtensionFromFileName() {
        Assertions.assertEquals("fileName", UriUtils.removeExtension("fileName.csv"));
    }

    @Test
    @DisplayName("Assert file name extracted from some cmd line")
    void extractFileNameFromCommandLine() {
        String cmdLine = Path.of(" home", "user", "someFile.jar").toString() + " -arg1 val";
        Assertions.assertEquals(UriUtils.stripFileNameFromCommandLine(cmdLine), "someFile.jar");
    }

    @NotNull
    private BasicHttpResponse getBasicHttpResponse(int statusCode) {
        BasicHttpResponse basicHttpResponse = new BasicHttpResponse(new BasicStatusLine(new ProtocolVersion("http", 1, 1), statusCode, "Some Error"));
        basicHttpResponse.addHeader("x-ms-activity-id", "1234");
        return basicHttpResponse;
    }
}
