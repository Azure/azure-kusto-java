package com.microsoft.azure.kusto.ingest;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IngestClientBaseTest {
    private static Stream<Arguments> provideStringsForAutoCorrectEndpointTruePass() {
        return Stream.of(
                Arguments.of("https://testendpoint.dev.kusto.windows.net", "https://ingest-testendpoint.dev.kusto.windows.net"),
                Arguments.of("https://shouldwork", "https://ingest-shouldwork"),
                Arguments.of("https://192.shouldwork.1.1", "https://ingest-192.shouldwork.1.1"),
                Arguments.of("https://2345:shouldwork:0425", "https://ingest-2345:shouldwork:0425"),
                Arguments.of("https://376.568.1564.1564", "https://ingest-376.568.1564.1564"),
                Arguments.of("https://192.168.1.1", "https://192.168.1.1"),
                Arguments.of("https://2345:0425:2CA1:0000:0000:0567:5673:23b5", "https://2345:0425:2CA1:0000:0000:0567:5673:23b5"),
                Arguments.of("https://127.0.0.1", "https://127.0.0.1"),
                Arguments.of("https://localhost", "https://localhost"),
                Arguments.of("https://onebox.dev.kusto.windows.net", "https://onebox.dev.kusto.windows.net"));
    }

    @ParameterizedTest
    @MethodSource("provideStringsForAutoCorrectEndpointTruePass")
    void autoCorrectEndpoint_True_Pass(String csb, String expected) {
        String actual = IngestClientBase.getIngestionEndpoint(csb);
        assertEquals(expected, actual);
    }
}
