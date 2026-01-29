// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

/**
 * Tests for [IngestClientBase] URL handling utilities.
 *
 * These tests match the Java IngestClientBaseTest to ensure parity.
 */
class IngestClientBaseTest {

    companion object {
        @JvmStatic
        fun provideStringsForGetIngestionEndpoint(): Stream<Arguments> =
            Stream.of(
                // Normal URLs should get ingest- prefix
                Arguments.of(
                    "https://testendpoint.dev.kusto.windows.net",
                    "https://ingest-testendpoint.dev.kusto.windows.net",
                ),
                Arguments.of(
                    "https://shouldwork",
                    "https://ingest-shouldwork",
                ),
                // Non-IP hostnames that look like IPs should get prefix
                Arguments.of(
                    "https://192.shouldwork.1.1",
                    "https://ingest-192.shouldwork.1.1",
                ),
                Arguments.of(
                    "https://2345:shouldwork:0425",
                    "https://ingest-2345:shouldwork:0425",
                ),
                Arguments.of(
                    "https://376.568.1564.1564",
                    "https://ingest-376.568.1564.1564",
                ),
                // Valid IPv4 addresses should NOT get prefix
                Arguments.of(
                    "https://192.168.1.1",
                    "https://192.168.1.1",
                ),
                Arguments.of(
                    "https://127.0.0.1",
                    "https://127.0.0.1",
                ),
                // IPv6 addresses should NOT get prefix
                Arguments.of(
                    "https://[2345:0425:2CA1:0000:0000:0567:5673:23b5]",
                    "https://[2345:0425:2CA1:0000:0000:0567:5673:23b5]",
                ),
                // Localhost should NOT get prefix
                Arguments.of(
                    "https://localhost",
                    "https://localhost",
                ),
                // Onebox dev should NOT get prefix
                Arguments.of(
                    "https://onebox.dev.kusto.windows.net",
                    "https://onebox.dev.kusto.windows.net",
                ),
            )

        @JvmStatic
        fun provideStringsForGetQueryEndpoint(): Stream<Arguments> =
            Stream.of(
                // Should remove ingest- prefix
                Arguments.of(
                    "https://ingest-testendpoint.dev.kusto.windows.net",
                    "https://testendpoint.dev.kusto.windows.net",
                ),
                // No ingest- prefix should return unchanged
                Arguments.of(
                    "https://testendpoint.dev.kusto.windows.net",
                    "https://testendpoint.dev.kusto.windows.net",
                ),
                // Reserved hostnames should return unchanged
                Arguments.of(
                    "https://localhost",
                    "https://localhost",
                ),
                Arguments.of(
                    "https://127.0.0.1",
                    "https://127.0.0.1",
                ),
                Arguments.of(
                    "https://onebox.dev.kusto.windows.net",
                    "https://onebox.dev.kusto.windows.net",
                ),
            )
    }

    @ParameterizedTest
    @MethodSource("provideStringsForGetIngestionEndpoint")
    fun `getIngestionEndpoint should correctly transform URLs`(
        input: String,
        expected: String,
    ) {
        val actual = IngestClientBase.getIngestionEndpoint(input)
        assertEquals(expected, actual)
    }

    @ParameterizedTest
    @MethodSource("provideStringsForGetQueryEndpoint")
    fun `getQueryEndpoint should correctly transform URLs`(
        input: String,
        expected: String,
    ) {
        val actual = IngestClientBase.getQueryEndpoint(input)
        assertEquals(expected, actual)
    }

    @Test
    fun `getIngestionEndpoint with null should return null`() {
        assertNull(IngestClientBase.getIngestionEndpoint(null))
    }

    @Test
    fun `getQueryEndpoint with null should return null`() {
        assertNull(IngestClientBase.getQueryEndpoint(null))
    }

    @Test
    fun `getIngestionEndpoint with existing ingest prefix should return unchanged`() {
        val url = "https://ingest-test.kusto.windows.net"
        assertEquals(url, IngestClientBase.getIngestionEndpoint(url))
    }

    @Test
    fun `getIngestionEndpoint without protocol should return unchanged`() {
        // URLs without protocol are treated as non-absolute URIs (reserved)
        // and returned unchanged - matching Java IngestClientBase behavior
        assertEquals(
            "test.kusto.windows.net",
            IngestClientBase.getIngestionEndpoint("test.kusto.windows.net"),
        )
    }

    @Test
    fun `isReservedHostname for localhost should return true`() {
        assertTrue(IngestClientBase.isReservedHostname("https://localhost"))
        assertTrue(IngestClientBase.isReservedHostname("https://localhost:8080"))
        assertTrue(
            IngestClientBase.isReservedHostname("https://localhost/path"),
        )
    }

    @Test
    fun `isReservedHostname for valid IPv4 should return true`() {
        assertTrue(IngestClientBase.isReservedHostname("https://192.168.1.1"))
        assertTrue(IngestClientBase.isReservedHostname("https://10.0.0.1"))
        assertTrue(IngestClientBase.isReservedHostname("https://127.0.0.1"))
    }

    @Test
    fun `isReservedHostname for IPv6 should return true`() {
        assertTrue(
            IngestClientBase.isReservedHostname(
                "https://[2345:0425:2CA1:0000:0000:0567:5673:23b5]",
            ),
        )
        assertTrue(IngestClientBase.isReservedHostname("https://[::1]"))
    }

    @Test
    fun `isReservedHostname for onebox dev should return true`() {
        assertTrue(
            IngestClientBase.isReservedHostname(
                "https://onebox.dev.kusto.windows.net",
            ),
        )
        assertTrue(
            IngestClientBase.isReservedHostname(
                "https://ONEBOX.DEV.KUSTO.WINDOWS.NET",
            ),
        )
    }

    @Test
    fun `isReservedHostname for normal URLs should return false`() {
        assertFalse(
            IngestClientBase.isReservedHostname(
                "https://test.kusto.windows.net",
            ),
        )
        assertFalse(
            IngestClientBase.isReservedHostname(
                "https://ingest-test.kusto.windows.net",
            ),
        )
    }

    @Test
    fun `isReservedHostname for invalid IPv4-like strings should return false`() {
        // These look like IPs but aren't valid (out of range or wrong format)
        assertFalse(
            IngestClientBase.isReservedHostname("https://376.568.1564.1564"),
        )
        assertFalse(
            IngestClientBase.isReservedHostname("https://192.shouldwork.1.1"),
        )
    }

    @Test
    fun `isReservedHostname for non-absolute URI should return true`() {
        assertTrue(IngestClientBase.isReservedHostname("not-a-valid-uri"))
        assertTrue(IngestClientBase.isReservedHostname("test.kusto.windows.net"))
    }
}