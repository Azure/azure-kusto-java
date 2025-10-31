// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.utils

import java.net.URI
import java.util.*
import java.util.regex.Pattern

object PathUtils {
    private const val PREFIX = "Ingest.V2.Java"
    private const val FILE_NAME_SEGMENT_MAX_LENGTH = 120
    private const val TOTAL_TWO_SEGMENT_MAX_LENGTH = 160
    private const val TRUNCATION_SUFFIX = "__trunc"
    private val URI_FRAGMENT_SEPARATORS = charArrayOf('?', '#', ';')

    // Only allow a-z, A-Z, 0-9, and hyphen (-) in the sanitized file name.
    private val FORBIDDEN_CHARS =
        Pattern.compile("[^\\w-]", Pattern.CASE_INSENSITIVE)

    fun sanitizeFileName(baseName: String?, sourceId: String?): String {
        val base = getBasename(baseName)
        val fileNameSegment = sanitize(base, FILE_NAME_SEGMENT_MAX_LENGTH)
        val baseNamePart =
            if (!base.isNullOrEmpty()) "_$fileNameSegment" else ""
        return sanitize(
            sourceId,
            TOTAL_TWO_SEGMENT_MAX_LENGTH - fileNameSegment.length,
        ) + baseNamePart
    }

    private fun sanitize(name: String?, maxSize: Int): String {
        if (name.isNullOrEmpty()) return ""
        var sanitized = FORBIDDEN_CHARS.matcher(name).replaceAll("-")
        if (sanitized.length > maxSize) {
            sanitized =
                sanitized.take(maxSize - TRUNCATION_SUFFIX.length) +
                TRUNCATION_SUFFIX
        }
        return sanitized
    }

    // Format: Ingest.V2.Dotnet_{timestamp}_{random}_{format}_{sourceId}_{name}
    // Sample:
    // Ingest.V2.Dotnet_20250702080158084_874b2e9373414f64aa5a9f9c0d240b07_file_e493b23d-684f-4f4c-8ba8-3edfaca09427_dataset-json.multijson.gz
    fun createFileNameForUpload(name: String): String {
        val timestamp =
            java.time.format.DateTimeFormatter.ofPattern(
                "yyyyMMddHHmmssSSS",
            )
                .format(
                    java.time.Instant.now()
                        .atZone(java.time.ZoneOffset.UTC),
                )
        return PREFIX +
            "_$timestamp" +
            "_${UUID.randomUUID().toString().replace("-", "")}" +
            "_$name"
    }

    /**
     * Returns the base name of the file, with extensions and without any path.
     * Works for both local paths and URLs. Examples:
     * - "C:\path\to\file.csv.gz" -> "file.csv.gz"
     * - "https://example.com/path/to/file.csv.gz" -> "file.csv.gz"
     */
    fun getBasename(uri: String?): String? {
        if (uri.isNullOrBlank()) return uri
        val uriObj =
            try {
                URI(uri)
            } catch (e: Exception) {
                null
            }
        if (uriObj == null || !uriObj.isAbsolute) {
            // Not a valid absolute URI, treat as path
            // Chain substringAfterLast for both '/' and '\' correctly
            return uri.substringAfterLast('/').substringAfterLast('\\')
        }
        // For web URIs, extract last segment of the path, remove query/fragment
        val path = uriObj.path ?: ""
        var lastSegment = path.split('/', '\\').lastOrNull() ?: ""
        val queryIndex = lastSegment.indexOfAny(URI_FRAGMENT_SEPARATORS)
        if (queryIndex >= 0) {
            lastSegment = lastSegment.take(queryIndex)
        }
        return lastSegment
    }
}
