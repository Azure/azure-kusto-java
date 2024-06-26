package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import com.microsoft.azure.kusto.data.http.HttpPostUtils;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.Objects;

class UtilitiesTest {
    @Test
    @DisplayName("Test exception creation when the web response is null")
    void createExceptionFromResponseNoResponse() {
        DataServiceException error = HttpPostUtils.createExceptionFromResponse("https://sample.kusto.windows.net", null, new Exception(), "error");
        Assertions.assertEquals("POST failed to send request", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
    }

    @Test
    @DisplayName("Test exception creation on a 404 error")
    void createExceptionFromResponse404Error() {
        BasicHttpResponse basicHttpResponse = getBasicHttpResponse(404);
        DataServiceException error = HttpPostUtils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(), "error");
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
        DataServiceException error = HttpPostUtils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                OneApiError);
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
        DataServiceException error = HttpPostUtils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                errorMessage);
        Assertions.assertEquals("Test Error Message, ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(401, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from a bad json")
    void createExceptionFromBadJson() {
        String errorMessage = "\"message\": \"Test Error Message\"";
        BasicHttpResponse basicHttpResponse = getBasicHttpResponse(401);
        DataServiceException error = HttpPostUtils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                errorMessage);
        Assertions.assertEquals("\"message\": \"Test Error Message\", ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(401, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from an unexpected json")
    void createExceptionFromOtherJson() {
        String errorMessage = "{\"response\": \"Test Error Message\"}";
        BasicHttpResponse basicHttpResponse = getBasicHttpResponse(401);
        DataServiceException error = HttpPostUtils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                errorMessage);
        Assertions.assertEquals("{\"response\": \"Test Error Message\"}, ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(401, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from a blank error message")
    void createExceptionFromBlankErrorMessage() {
        String errorMessage = " ";
        BasicHttpResponse basicHttpResponse = getBasicHttpResponse(401);
        DataServiceException error = HttpPostUtils.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                errorMessage);
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
        String cmdLine = Paths.get(" home", "user", "someFile.jar").toString() + " -arg1 val";
        Assertions.assertEquals(UriUtils.stripFileNameFromCommandLine(cmdLine), "someFile.jar");
    }

    @Test
    @DisplayName("Test exception creation from a message object")
    void isRetrieable() {
        IOException e = new UnknownHostException("Doesnt exist");
        Assertions.assertFalse(Utils.isRetriableIOException(e));

        e = new org.apache.http.conn.HttpHostConnectException(new ConnectException("Connection refused"), null);
        Assertions.assertFalse(Utils.isRetriableIOException(e));

        e = new org.apache.http.conn.HttpHostConnectException(new ConnectException("Connection timed out"), null);
        Assertions.assertTrue(Utils.isRetriableIOException(e));
    }

    @NotNull
    private BasicHttpResponse getBasicHttpResponse(int statusCode) {
        BasicHttpResponse basicHttpResponse = new BasicHttpResponse(new BasicStatusLine(new ProtocolVersion("http", 1, 1), statusCode, "Some Error"));
        basicHttpResponse.addHeader("x-ms-activity-id", "1234");
        return basicHttpResponse;
    }
}
