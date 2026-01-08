// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.utils

import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime

class IngestionResultUtilsTest {

    private fun createBlobStatus(
        status: BlobStatus.Status,
        sourceId: String = "test-id",
    ): BlobStatus {
        return BlobStatus(
            sourceId = sourceId,
            status = status,
            startedAt = OffsetDateTime.now(),
            lastUpdateTime = OffsetDateTime.now(),
            errorCode = null,
            failureStatus = null,
            details = null,
        )
    }

    @Test
    fun `hasFailedResults returns true when list contains failed status`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded),
                createBlobStatus(BlobStatus.Status.Failed),
                createBlobStatus(BlobStatus.Status.Queued),
            )

        assertTrue(IngestionResultUtils.hasFailedResults(results))
    }

    @Test
    fun `hasFailedResults returns false when list has no failed status`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded),
                createBlobStatus(BlobStatus.Status.Queued),
                createBlobStatus(BlobStatus.Status.InProgress),
            )

        assertFalse(IngestionResultUtils.hasFailedResults(results))
    }

    @Test
    fun `hasFailedResults returns false for empty list`() {
        val results = emptyList<BlobStatus>()

        assertFalse(IngestionResultUtils.hasFailedResults(results))
    }

    @Test
    fun `isCompleted returns true when all results are in terminal states`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded),
                createBlobStatus(BlobStatus.Status.Failed),
                createBlobStatus(BlobStatus.Status.Canceled),
            )

        assertTrue(IngestionResultUtils.isCompleted(results))
    }

    @Test
    fun `isCompleted returns false when some results are in progress`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded),
                createBlobStatus(BlobStatus.Status.InProgress),
                createBlobStatus(BlobStatus.Status.Failed),
            )

        assertFalse(IngestionResultUtils.isCompleted(results))
    }

    @Test
    fun `isCompleted returns false when some results are queued`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded),
                createBlobStatus(BlobStatus.Status.Queued),
            )

        assertFalse(IngestionResultUtils.isCompleted(results))
    }

    @Test
    fun `isCompleted returns false for null list`() {
        assertFalse(IngestionResultUtils.isCompleted(null))
    }

    @Test
    fun `isCompleted returns false for empty list`() {
        assertFalse(IngestionResultUtils.isCompleted(emptyList()))
    }

    @Test
    fun `isCompleted returns true when only succeeded results`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded),
                createBlobStatus(BlobStatus.Status.Succeeded),
            )

        assertTrue(IngestionResultUtils.isCompleted(results))
    }

    @Test
    fun `isCompleted returns true when only failed results`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Failed),
                createBlobStatus(BlobStatus.Status.Failed),
            )

        assertTrue(IngestionResultUtils.isCompleted(results))
    }

    @Test
    fun `isInProgress returns true when results contain queued status`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded),
                createBlobStatus(BlobStatus.Status.Queued),
            )

        assertTrue(IngestionResultUtils.isInProgress(results))
    }

    @Test
    fun `isInProgress returns true when results contain in progress status`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded),
                createBlobStatus(BlobStatus.Status.InProgress),
            )

        assertTrue(IngestionResultUtils.isInProgress(results))
    }

    @Test
    fun `isInProgress returns false when all results are terminal`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded),
                createBlobStatus(BlobStatus.Status.Failed),
                createBlobStatus(BlobStatus.Status.Canceled),
            )

        assertFalse(IngestionResultUtils.isInProgress(results))
    }

    @Test
    fun `isInProgress returns false for null list`() {
        assertFalse(IngestionResultUtils.isInProgress(null))
    }

    @Test
    fun `isInProgress returns false for empty list`() {
        assertFalse(IngestionResultUtils.isInProgress(emptyList()))
    }

    @Test
    fun `getFailedResults returns only failed results`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded, "id1"),
                createBlobStatus(BlobStatus.Status.Failed, "id2"),
                createBlobStatus(BlobStatus.Status.Failed, "id3"),
                createBlobStatus(BlobStatus.Status.Queued, "id4"),
            )

        val failedResults = IngestionResultUtils.getFailedResults(results)

        assertEquals(2, failedResults.size)
        assertTrue(failedResults.all { it.status == BlobStatus.Status.Failed })
        assertEquals("id2", failedResults[0].sourceId)
        assertEquals("id3", failedResults[1].sourceId)
    }

    @Test
    fun `getFailedResults returns empty list when no failures`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded),
                createBlobStatus(BlobStatus.Status.Queued),
            )

        val failedResults = IngestionResultUtils.getFailedResults(results)

        assertTrue(failedResults.isEmpty())
    }

    @Test
    fun `getFailedResults returns empty list for null input`() {
        val failedResults = IngestionResultUtils.getFailedResults(null)

        assertTrue(failedResults.isEmpty())
    }

    @Test
    fun `getSucceededResults returns only succeeded results`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded, "id1"),
                createBlobStatus(BlobStatus.Status.Failed, "id2"),
                createBlobStatus(BlobStatus.Status.Succeeded, "id3"),
                createBlobStatus(BlobStatus.Status.Queued, "id4"),
            )

        val succeededResults = IngestionResultUtils.getSucceededResults(results)

        assertEquals(2, succeededResults.size)
        assertTrue(
            succeededResults.all {
                it.status == BlobStatus.Status.Succeeded
            },
        )
        assertEquals("id1", succeededResults[0].sourceId)
        assertEquals("id3", succeededResults[1].sourceId)
    }

    @Test
    fun `getSucceededResults returns empty list when no successes`() {
        val results =
            listOf(
                createBlobStatus(BlobStatus.Status.Failed),
                createBlobStatus(BlobStatus.Status.Queued),
            )

        val succeededResults = IngestionResultUtils.getSucceededResults(results)

        assertTrue(succeededResults.isEmpty())
    }

    @Test
    fun `getSucceededResults returns empty list for null input`() {
        val succeededResults = IngestionResultUtils.getSucceededResults(null)

        assertTrue(succeededResults.isEmpty())
    }

    @Test
    fun `test mixed status scenarios`() {
        val allStatuses =
            listOf(
                createBlobStatus(BlobStatus.Status.Succeeded, "id1"),
                createBlobStatus(BlobStatus.Status.Failed, "id2"),
                createBlobStatus(BlobStatus.Status.Queued, "id3"),
                createBlobStatus(BlobStatus.Status.InProgress, "id4"),
                createBlobStatus(BlobStatus.Status.Canceled, "id5"),
            )

        assertTrue(IngestionResultUtils.hasFailedResults(allStatuses))
        assertFalse(IngestionResultUtils.isCompleted(allStatuses))
        assertTrue(IngestionResultUtils.isInProgress(allStatuses))
        assertEquals(1, IngestionResultUtils.getFailedResults(allStatuses).size)
        assertEquals(
            1,
            IngestionResultUtils.getSucceededResults(allStatuses).size,
        )
    }
}
