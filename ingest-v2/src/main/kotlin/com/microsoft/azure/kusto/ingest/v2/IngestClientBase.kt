// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import java.net.URI

/**
 * Base utility object containing common functionality for ingest clients.
 *
 * This implementation matches the Java IngestClientBase behavior for handling
 * special URLs like localhost, IP addresses, and reserved hostnames.
 */
object IngestClientBase {
    private const val INGEST_PREFIX = "ingest-"
    private const val PROTOCOL_SUFFIX = "://"

    /**
     * Converts a cluster URL to an ingestion endpoint URL by adding the
     * "ingest-" prefix.
     *
     * Special URLs (localhost, IP addresses, onebox.dev.kusto.windows.net) are
     * returned unchanged to support local development and testing scenarios.
     *
     * @param clusterUrl The cluster URL to convert
     * @return The ingestion endpoint URL with "ingest-" prefix, or the original
     *   URL for special cases
     */
    @JvmStatic
    fun getIngestionEndpoint(clusterUrl: String?): String? {
        if (clusterUrl == null ||
            clusterUrl.contains(INGEST_PREFIX) ||
            isReservedHostname(clusterUrl)
        ) {
            return clusterUrl
        }
        return if (clusterUrl.contains(PROTOCOL_SUFFIX)) {
            clusterUrl.replaceFirst(
                PROTOCOL_SUFFIX,
                "$PROTOCOL_SUFFIX$INGEST_PREFIX",
            )
        } else {
            INGEST_PREFIX + clusterUrl
        }
    }

    /**
     * Converts an ingestion endpoint URL to a query endpoint URL by removing
     * the "ingest-" prefix.
     *
     * Special URLs (localhost, IP addresses, onebox.dev.kusto.windows.net) are
     * returned unchanged.
     *
     * @param clusterUrl The ingestion endpoint URL to convert
     * @return The query endpoint URL without "ingest-" prefix, or the original
     *   URL for special cases
     */
    @JvmStatic
    fun getQueryEndpoint(clusterUrl: String?): String? {
        return if (clusterUrl == null || isReservedHostname(clusterUrl)) {
            clusterUrl
        } else {
            clusterUrl.replaceFirst(INGEST_PREFIX, "")
        }
    }

    /**
     * Checks if the given URL points to a reserved hostname that should not
     * have the "ingest-" prefix added.
     *
     * Reserved hostnames include:
     * - localhost
     * - IPv4 addresses (e.g., 127.0.0.1, 192.168.1.1)
     * - IPv6 addresses (e.g., [2345:0425:2CA1:0000:0000:0567:5673:23b5])
     * - onebox.dev.kusto.windows.net (development environment)
     * - Non-absolute URIs
     *
     * @param rawUri The URL to check
     * @return true if the URL is a reserved hostname, false otherwise
     */
    @JvmStatic
    fun isReservedHostname(rawUri: String): Boolean {
        val uri =
            try {
                URI.create(rawUri)
            } catch (_: IllegalArgumentException) {
                return true
            }

        if (!uri.isAbsolute) {
            return true
        }

        val authority = uri.authority?.lowercase() ?: return true

        // Check for IPv6 address (wrapped in brackets)
        val isIpAddress =
            if (authority.startsWith("[") && authority.endsWith("]")) {
                true
            } else {
                isIPv4Address(authority)
            }

        val isLocalhost = authority.contains("localhost")
        val host = uri.host?.lowercase() ?: ""

        return isLocalhost ||
            isIpAddress ||
            host.equals("onebox.dev.kusto.windows.net", ignoreCase = true)
    }

    /**
     * Checks if the given string is a valid IPv4 address.
     *
     * This method validates that the string consists of exactly 4 octets, each
     * being a number between 0 and 255.
     *
     * @param address The string to check (may include port like "127.0.0.1:8080")
     * @return true if the string is a valid IPv4 address, false otherwise
     */
    private fun isIPv4Address(address: String): Boolean {
        // Remove port if present (e.g., "127.0.0.1:8080" -> "127.0.0.1")
        val hostPart =
            if (address.contains(":")) {
                address.substringBeforeLast(":")
            } else {
                address
            }

        val parts = hostPart.split(".")
        if (parts.size != 4) {
            return false
        }

        return parts.all { part ->
            val num = part.toIntOrNull()
            num != null && num in 0..255
        }
    }
}