// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2.common.exceptions

import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadErrorCode
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Unit tests for IngestException and its subclasses.
 */
class IngestExceptionTest {

    // ==================== IngestException Tests ====================

    @Test
    fun `IngestException should use provided message`() {
        val exception = IngestException("Test error message")
        assertEquals("Test error message", exception.message)
    }

    @Test
    fun `IngestException should use fallback message when null`() {
        val exception = IngestException()
        assertEquals(
            "Something went wrong calling Kusto client library (fallback message).",
            exception.message
        )
    }

    @Test
    fun `IngestException should store cause`() {
        val cause = IOException("Original error")
        val exception = IngestException("Wrapper", cause)

        assertEquals("Wrapper", exception.message)
        assertSame(cause, exception.cause)
    }

    @Test
    fun `IngestException should store failure code`() {
        val exception = IngestException(failureCode = 500)
        assertEquals(500, exception.failureCode)
    }

    @Test
    fun `IngestException should store failure sub code`() {
        val exception = IngestException(failureSubCode = "SubCode123")
        assertEquals("SubCode123", exception.failureSubCode)
    }

    @Test
    fun `IngestException should store isPermanent flag`() {
        val permanentException = IngestException(isPermanent = true)
        assertTrue(permanentException.isPermanent == true)

        val transientException = IngestException(isPermanent = false)
        assertFalse(transientException.isPermanent == true)
    }

    @Test
    fun `IngestException toString should return message`() {
        val exception = IngestException("Error message")
        assertEquals("Error message", exception.toString())
    }

    // ==================== IngestRequestException Tests ====================

    @Test
    fun `IngestRequestException should format message correctly`() {
        val exception = IngestRequestException(
            errorCode = "ERR001",
            errorReason = "Bad Request",
            errorMessage = "Invalid parameter"
        )

        assertTrue(exception.message.contains("Bad Request"))
        assertTrue(exception.message.contains("ERR001"))
        assertTrue(exception.message.contains("Invalid parameter"))
    }

    @Test
    fun `IngestRequestException should use custom message when provided`() {
        val exception = IngestRequestException(
            errorCode = "ERR001",
            errorReason = "Bad Request",
            errorMessage = "Invalid parameter",
            message = "Custom error message"
        )

        assertEquals("Custom error message", exception.message)
    }

    @Test
    fun `IngestRequestException should store all properties`() {
        val cause = RuntimeException("Cause")
        val exception = IngestRequestException(
            errorCode = "ERR001",
            errorReason = "Bad Request",
            errorMessage = "Invalid parameter",
            dataSource = "test-source",
            databaseName = "test-db",
            clientRequestId = "req-123",
            activityId = "act-456",
            failureCode = 400,
            failureSubCode = "SUB001",
            isPermanent = true,
            cause = cause
        )

        assertEquals("ERR001", exception.errorCode)
        assertEquals("Bad Request", exception.errorReason)
        assertEquals("Invalid parameter", exception.errorMessage)
        assertEquals("test-source", exception.dataSource)
        assertEquals("test-db", exception.databaseName)
        assertEquals("req-123", exception.clientRequestId)
        assertEquals("act-456", exception.activityId)
        assertEquals(400, exception.failureCode)
        assertTrue(exception.isPermanent == true)
        assertSame(cause, exception.cause)
    }

    @Test
    fun `IngestRequestException should default isPermanent to true`() {
        val exception = IngestRequestException()
        assertTrue(exception.isPermanent == true)
    }

    // ==================== IngestServiceException Tests ====================

    @Test
    fun `IngestServiceException should format message correctly`() {
        val exception = IngestServiceException(
            errorCode = "SVC001",
            errorReason = "Service Unavailable",
            errorMessage = "Server is busy"
        )

        assertTrue(exception.message.contains("Service Unavailable"))
        assertTrue(exception.message.contains("SVC001"))
        assertTrue(exception.message.contains("Server is busy"))
        assertTrue(exception.message.contains("temporary"))
    }

    @Test
    fun `IngestServiceException should use custom message when provided`() {
        val exception = IngestServiceException(
            errorCode = "SVC001",
            message = "Custom service error"
        )

        assertEquals("Custom service error", exception.message)
    }

    @Test
    fun `IngestServiceException should store all properties`() {
        val exception = IngestServiceException(
            errorCode = "SVC001",
            errorReason = "Service Error",
            errorMessage = "Internal error",
            dataSource = "kusto-cluster",
            clientRequestId = "client-req-1",
            activityId = "activity-1",
            failureCode = 503,
            failureSubCode = "RETRY",
            isPermanent = false
        )

        assertEquals("SVC001", exception.errorCode)
        assertEquals("Service Error", exception.errorReason)
        assertEquals("Internal error", exception.errorMessage)
        assertEquals("kusto-cluster", exception.dataSource)
        assertEquals("client-req-1", exception.clientRequestId)
        assertEquals("activity-1", exception.activityId)
        assertEquals(503, exception.failureCode)
        assertEquals("RETRY", exception.failureSubCode)
        assertFalse(exception.isPermanent == true)
    }

    @Test
    fun `IngestServiceException should default failureCode to 500`() {
        val exception = IngestServiceException()
        assertEquals(500, exception.failureCode)
    }

    // ==================== IngestClientException Tests ====================

    @Test
    fun `IngestClientException should format message correctly`() {
        val exception = IngestClientException(
            ingestionSource = "test-file.csv",
            error = "File not found"
        )

        assertTrue(exception.message.contains("test-file.csv"))
        assertTrue(exception.message.contains("File not found"))
    }

    @Test
    fun `IngestClientException should use custom message when provided`() {
        val exception = IngestClientException(
            ingestionSource = "test-file.csv",
            error = "File not found",
            message = "Custom client error"
        )

        assertEquals("Custom client error", exception.message)
    }

    @Test
    fun `IngestClientException should store all properties`() {
        val exception = IngestClientException(
            ingestionSourceId = "source-123",
            ingestionSource = "data.json",
            error = "Parse error",
            failureCode = 400,
            failureSubCode = "PARSE",
            isPermanent = true
        )

        assertEquals("source-123", exception.ingestionSourceId)
        assertEquals("data.json", exception.ingestionSource)
        assertEquals("Parse error", exception.error)
        assertEquals(400, exception.failureCode)
        assertEquals("PARSE", exception.failureSubCode)
        assertTrue(exception.isPermanent == true)
    }

    @Test
    fun `IngestClientException should default failureCode to 400`() {
        val exception = IngestClientException()
        assertEquals(400, exception.failureCode)
    }

    // ==================== IngestSizeLimitExceededException Tests ====================

    @Test
    fun `IngestSizeLimitExceededException should format message correctly`() {
        val exception = IngestSizeLimitExceededException(
            size = 1000000,
            maxNumberOfBlobs = 500000,
            ingestionSource = "large-file.csv"
        )

        assertTrue(exception.message.contains("large-file.csv"))
        assertTrue(exception.message.contains("1000000"))
        assertTrue(exception.message.contains("500000"))
    }

    @Test
    fun `IngestSizeLimitExceededException should use custom message when provided`() {
        val exception = IngestSizeLimitExceededException(
            size = 1000000,
            maxNumberOfBlobs = 500000,
            message = "Custom size limit message"
        )

        assertEquals("Custom size limit message", exception.message)
    }

    @Test
    fun `IngestSizeLimitExceededException should store size properties`() {
        val exception = IngestSizeLimitExceededException(
            size = 2000000,
            maxNumberOfBlobs = 1000000
        )

        assertEquals(2000000, exception.size)
        assertEquals(1000000, exception.maxNumberOfBlobs)
    }

    @Test
    fun `IngestSizeLimitExceededException should default isPermanent to true`() {
        val exception = IngestSizeLimitExceededException(size = 100, maxNumberOfBlobs = 50)
        assertTrue(exception.isPermanent == true)
    }

    // ==================== InvalidIngestionMappingException Tests ====================

    @Test
    fun `InvalidIngestionMappingException should format message correctly`() {
        val exception = InvalidIngestionMappingException(
            ingestionSource = "data.json",
            error = "Missing required column"
        )

        assertTrue(exception.message.contains("Ingestion mapping is invalid"))
    }

    @Test
    fun `InvalidIngestionMappingException should use custom message when provided`() {
        val exception = InvalidIngestionMappingException(
            message = "Custom mapping error"
        )

        assertEquals("Custom mapping error", exception.message)
    }

    @Test
    fun `InvalidIngestionMappingException should default isPermanent to true`() {
        val exception = InvalidIngestionMappingException()
        assertTrue(exception.isPermanent == true)
    }

    // ==================== MultipleIngestionMappingPropertiesException Tests ====================

    @Test
    fun `MultipleIngestionMappingPropertiesException should format message correctly`() {
        val exception = MultipleIngestionMappingPropertiesException()

        assertTrue(exception.message.contains("At most one property"))
        assertTrue(exception.message.contains("ingestion mapping"))
    }

    @Test
    fun `MultipleIngestionMappingPropertiesException should use custom message when provided`() {
        val exception = MultipleIngestionMappingPropertiesException(
            message = "Custom multiple mapping error"
        )

        assertEquals("Custom multiple mapping error", exception.message)
    }

    @Test
    fun `MultipleIngestionMappingPropertiesException should default isPermanent to true`() {
        val exception = MultipleIngestionMappingPropertiesException()
        assertTrue(exception.isPermanent == true)
    }

    // ==================== UploadFailedException Tests ====================

    @Test
    fun `UploadFailedException should format message correctly`() {
        val exception = UploadFailedException(
            fileName = "test.csv",
            blobName = "container/blob.csv",
            failureSubCode = UploadErrorCode.UPLOAD_FAILED
        )

        assertTrue(exception.message.contains("test.csv"))
        assertTrue(exception.message.contains("container/blob.csv"))
    }

    @Test
    fun `UploadFailedException should use custom message when provided`() {
        val exception = UploadFailedException(
            fileName = "test.csv",
            blobName = "blob.csv",
            failureSubCode = UploadErrorCode.UPLOAD_FAILED,
            message = "Custom upload error"
        )

        assertEquals("Custom upload error", exception.message)
    }

    @Test
    fun `UploadFailedException should store all properties`() {
        val cause = IOException("Network error")
        val exception = UploadFailedException(
            fileName = "data.json",
            blobName = "container/data.json",
            failureCode = 500,
            failureSubCode = UploadErrorCode.NETWORK_ERROR,
            isPermanent = false,
            cause = cause
        )

        assertEquals("data.json", exception.fileName)
        assertEquals("container/data.json", exception.blobName)
        assertEquals(500, exception.failureCode)
        assertEquals(UploadErrorCode.NETWORK_ERROR.toString(), exception.failureSubCode)
        assertFalse(exception.isPermanent == true)
        assertSame(cause, exception.cause)
    }

    // ==================== NoAvailableIngestContainersException Tests ====================

    @Test
    fun `NoAvailableIngestContainersException should format message correctly`() {
        val exception = NoAvailableIngestContainersException(
            failureSubCode = UploadErrorCode.NO_CONTAINERS_AVAILABLE
        )

        assertTrue(exception.message.contains("No available containers"))
    }

    @Test
    fun `NoAvailableIngestContainersException should use custom message when provided`() {
        val exception = NoAvailableIngestContainersException(
            failureSubCode = UploadErrorCode.NO_CONTAINERS_AVAILABLE,
            message = "Custom no containers message"
        )

        assertEquals("Custom no containers message", exception.message)
    }

    @Test
    fun `NoAvailableIngestContainersException should default failureCode to 500`() {
        val exception = NoAvailableIngestContainersException(
            failureSubCode = UploadErrorCode.NO_CONTAINERS_AVAILABLE
        )
        assertEquals(500, exception.failureCode)
    }

    @Test
    fun `NoAvailableIngestContainersException should default isPermanent to false`() {
        val exception = NoAvailableIngestContainersException(
            failureSubCode = UploadErrorCode.NO_CONTAINERS_AVAILABLE
        )
        assertFalse(exception.isPermanent == true)
    }

    // ==================== InvalidUploadStreamException Tests ====================

    @Test
    fun `InvalidUploadStreamException should format message correctly`() {
        val exception = InvalidUploadStreamException(
            fileName = "empty.csv",
            failureSubCode = UploadErrorCode.SOURCE_IS_EMPTY
        )

        assertTrue(exception.message.contains("invalid"))
        assertTrue(exception.message.contains(UploadErrorCode.SOURCE_IS_EMPTY.toString()))
    }

    @Test
    fun `InvalidUploadStreamException should use custom message when provided`() {
        val exception = InvalidUploadStreamException(
            failureSubCode = UploadErrorCode.SOURCE_IS_EMPTY,
            message = "Custom invalid stream message"
        )

        assertEquals("Custom invalid stream message", exception.message)
    }

    @Test
    fun `InvalidUploadStreamException should default isPermanent to true`() {
        val exception = InvalidUploadStreamException(
            failureSubCode = UploadErrorCode.SOURCE_IS_NULL
        )
        assertTrue(exception.isPermanent == true)
    }

    // ==================== UploadSizeLimitExceededException Tests ====================

    @Test
    fun `UploadSizeLimitExceededException should format message correctly`() {
        val exception = UploadSizeLimitExceededException(
            size = 1000000,
            maxSize = 500000,
            fileName = "large.csv",
            failureSubCode = UploadErrorCode.SOURCE_SIZE_LIMIT_EXCEEDED
        )

        assertTrue(exception.message.contains("large.csv"))
        assertTrue(exception.message.contains("1000000"))
        assertTrue(exception.message.contains("500000"))
    }

    @Test
    fun `UploadSizeLimitExceededException should use custom message when provided`() {
        val exception = UploadSizeLimitExceededException(
            size = 1000000,
            maxSize = 500000,
            failureSubCode = UploadErrorCode.SOURCE_SIZE_LIMIT_EXCEEDED,
            message = "Custom size error"
        )

        assertEquals("Custom size error", exception.message)
    }

    @Test
    fun `UploadSizeLimitExceededException should store size properties`() {
        val exception = UploadSizeLimitExceededException(
            size = 2000000,
            maxSize = 1000000,
            failureSubCode = UploadErrorCode.SOURCE_SIZE_LIMIT_EXCEEDED
        )

        assertEquals(2000000, exception.size)
        assertEquals(1000000, exception.maxSize)
    }

    @Test
    fun `UploadSizeLimitExceededException should default isPermanent to true`() {
        val exception = UploadSizeLimitExceededException(
            size = 100,
            maxSize = 50,
            failureSubCode = UploadErrorCode.SOURCE_SIZE_LIMIT_EXCEEDED
        )
        assertTrue(exception.isPermanent == true)
    }
}
