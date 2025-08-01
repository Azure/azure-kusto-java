package com.microsoft.azure.kusto.data;

import com.azure.core.http.HttpResponse;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import com.microsoft.azure.kusto.data.http.HttpStatus;
import com.microsoft.azure.kusto.data.http.TestHttpResponse;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

class UtilitiesTest {
    private static final String TEST_STRING_WITH_MULTI_BYTE_CHARS = "Start -> äÄöÖüÜß€@'()<>!§$%&/{[]}?=´`+*~#-'_.:,; " +
            "<- Middle -> Zurich, Zürich, Straße, ÆØÅ æøå <- End.";

    @Test
    @DisplayName("Test exception creation when the web response is null")
    void createExceptionFromResponseNoResponse() {
        DataServiceException error = BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", null, new Exception(), "error");
        Assertions.assertEquals("POST failed to send request", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
    }

    @Test
    @DisplayName("Test exception creation on a 404 error")
    void createExceptionFromResponse404Error() {
        HttpResponse basicHttpResponse = getHttpResponse(HttpStatus.NOT_FOUND);
        DataServiceException error = BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(), "error");
        Assertions.assertTrue(error.getStatusCode() != null && error.getStatusCode() == HttpStatus.NOT_FOUND);
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
        HttpResponse basicHttpResponse = getHttpResponse(HttpStatus.UNAUTHORIZED);
        DataServiceException error = BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                OneApiError);
        Assertions.assertEquals("Query execution has exceeded the allowed limits (80DA0003): ., ActivityId='1234'", error.getMessage());
        Assertions.assertInstanceOf(DataWebException.class, error.getCause());
        Assertions.assertTrue(error.isPermanent());
        Assertions.assertEquals(HttpStatus.UNAUTHORIZED, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from a message object")
    void createExceptionFromMessageError() {
        String errorMessage = "{\"message\": \"Test Error Message\"}";
        HttpResponse basicHttpResponse = getHttpResponse(HttpStatus.UNAUTHORIZED);
        DataServiceException error = BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                errorMessage);
        Assertions.assertEquals("Test Error Message, ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(HttpStatus.UNAUTHORIZED, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from a bad json")
    void createExceptionFromBadJson() {
        String errorMessage = "\"message\": \"Test Error Message\"";
        HttpResponse basicHttpResponse = getHttpResponse(HttpStatus.UNAUTHORIZED);
        DataServiceException error = BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                errorMessage);
        Assertions.assertEquals("\"message\": \"Test Error Message\", ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(HttpStatus.UNAUTHORIZED, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from an unexpected json")
    void createExceptionFromOtherJson() {
        String errorMessage = "{\"response\": \"Test Error Message\"}";
        HttpResponse basicHttpResponse = getHttpResponse(HttpStatus.UNAUTHORIZED);
        DataServiceException error = BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                errorMessage);
        Assertions.assertEquals("{\"response\": \"Test Error Message\"}, ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(HttpStatus.UNAUTHORIZED, Objects.requireNonNull(error.getStatusCode()).intValue());
    }

    @Test
    @DisplayName("Test exception creation from non oneApiError")
    void createExceptionFromBlankErrorMessage() {
        String errorMessage = " ";
        HttpResponse basicHttpResponse = getHttpResponse(HttpStatus.UNAUTHORIZED);
        DataServiceException error = BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                errorMessage);
        Assertions.assertEquals("Http StatusCode='401', ActivityId='1234'", error.getMessage());
        Assertions.assertTrue(error.isPermanent());
        Assertions.assertEquals(HttpStatus.UNAUTHORIZED, Objects.requireNonNull(error.getStatusCode()).intValue());

        basicHttpResponse = getHttpResponse(HttpStatus.NOT_FOUND);
        error = BaseClient.createExceptionFromResponse("https://sample.kusto.windows.net", basicHttpResponse, new Exception(),
                errorMessage);
        Assertions.assertEquals("Http StatusCode='404', ActivityId='1234'", error.getMessage());
        Assertions.assertFalse(error.isPermanent());
        Assertions.assertEquals(HttpStatus.NOT_FOUND, Objects.requireNonNull(error.getStatusCode()).intValue());

    }

    @Test
    @DisplayName("Remove extension")
    void removeExtensionFromFileName() {
        Assertions.assertEquals("fileName", UriUtils.removeExtension("fileName.csv"));
    }

    @Test
    @DisplayName("Assert file name extracted from some cmd line")
    void extractFileNameFromCommandLine() {
        String cmdLine = Paths.get(" home", "user", "someFile.jar") + " -arg1 val";
        Assertions.assertEquals(UriUtils.stripFileNameFromCommandLine(cmdLine), "someFile.jar");
    }

    @Test
    @DisplayName("Test exception creation from a message object")
    void isRetrieable() {
        IOException e = new UnknownHostException("Doesnt exist");
        Assertions.assertFalse(Utils.isRetriableIOException(e));

        e = new ConnectException("Connection refused");
        Assertions.assertFalse(Utils.isRetriableIOException(e));

        e = new ConnectException("Connection timed out");
        Assertions.assertTrue(Utils.isRetriableIOException(e));
    }

    @NotNull
    private HttpResponse getHttpResponse(int statusCode) {
        return TestHttpResponse
                .newBuilder()
                .withStatusCode(statusCode)
                .addHeader("x-ms-activity-id", "1234")
                .build();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("getTestParameters")
    void shouldCorrectlyDecodeGzippedResponse(@NotNull String originalString, int chunkSize) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        if (!originalString.isEmpty()) {
            try (GZIPOutputStream gzipOut = new GZIPOutputStream(byteArrayOutputStream)) {
                gzipOut.write(originalString.getBytes(StandardCharsets.UTF_8));
            }
        }
        byte[] compressedBytes = byteArrayOutputStream.toByteArray();

        // Split the compressed data into chunks
        List<ByteBuffer> chunks = new ArrayList<>();
        if (compressedBytes.length > 0) {
            for (int i = 0; i < compressedBytes.length; i += chunkSize) {
                int end = Math.min(compressedBytes.length, i + chunkSize);
                chunks.add(ByteBuffer.wrap(compressedBytes, i, end - i));
            }
        }
        Flux<ByteBuffer> chunkedGzipStream = Flux.fromIterable(chunks);

        Mono<String> decodedString = Utils.processGzipBody(chunkedGzipStream);

        StepVerifier.create(decodedString)
                .expectNext(originalString)
                .verifyComplete();
    }

    @Test
    void shouldReturnEmptyStringForEmptyFlux() {
        Mono<String> resultMono = Utils.processGzipBody(Flux.empty());

        StepVerifier.create(resultMono)
                .expectNext(StringUtils.EMPTY)
                .verifyComplete();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("getTestParameters")
    void shouldCorrectlyDecodeNonGzippedStream(@NotNull String originalString, int chunkSize) {
        byte[] originalBytes = originalString.getBytes(StandardCharsets.UTF_8);

        // Split the raw bytes into chunks to simulate a network stream.
        List<ByteBuffer> chunks = new ArrayList<>();
        if (originalBytes.length > 0) {
            for (int i = 0; i < originalBytes.length; i += chunkSize) {
                int end = Math.min(originalBytes.length, i + chunkSize);
                chunks.add(ByteBuffer.wrap(originalBytes, i, end - i));
            }
        }
        Flux<ByteBuffer> chunkedStream = Flux.fromIterable(chunks);

        Mono<String> result = Utils.processNonGzipBody(chunkedStream);

        StepVerifier.create(result)
                .expectNext(originalString)
                .verifyComplete();
    }

    private static Stream<Arguments> getTestParameters() {
        return Stream.of(
                arguments(TEST_STRING_WITH_MULTI_BYTE_CHARS, 1),
                arguments(TEST_STRING_WITH_MULTI_BYTE_CHARS, 5),
                arguments(TEST_STRING_WITH_MULTI_BYTE_CHARS, 1024),
                arguments("", 5),
                arguments("Hello world, this is a simple test.", 5)
        );
    }
}
