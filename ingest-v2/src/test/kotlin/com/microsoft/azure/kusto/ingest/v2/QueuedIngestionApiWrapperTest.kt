// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.client.ingestProperties
import com.microsoft.azure.kusto.ingest.v2.common.IngestionMethod
import com.microsoft.azure.kusto.ingest.v2.common.auth.AzCliTokenCredentialsProvider
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.source.DataFormat
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.slf4j.LoggerFactory
import java.net.ConnectException
import kotlin.test.assertNotNull
import kotlin.test.fail

class QueuedIngestionApiWrapperTest {

    private val logger = LoggerFactory.getLogger(QueuedIngestionApiWrapperTest::class.java)

    @Test
    fun `test queued ingestion with CSV blob`(): Unit = runBlocking {
        // Skip test if no DM_CONNECTION_STRING is set
        val cluster = System.getenv("DM_CONNECTION_STRING")
        if (cluster == null) {
            assumeTrue(false, "Skipping test: No DM_CONNECTION_STRING environment variable set for real cluster testing")
            return@runBlocking
        }

        val actualTokenProvider = AzCliTokenCredentialsProvider()
        val actualWrapper = QueuedIngestionApiWrapper(
            dmUrl = cluster,
            tokenCredentialsProvider = actualTokenProvider,
            skipSecurityChecks = true
        )

        // Test parameters - using CSV blob storage test data with SAS token
        val database = System.getenv("TEST_DATABASE") ?: "e2e"
        val table = System.getenv("TEST_TABLE") ?: "CovidTracking"
        val testBlobUrls = listOf(
            "https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/covid_tracking/latest/covid_tracking.csv"
        )
        val format = DataFormat.CSV
        val properties = ingestProperties {
            enableTracking(true)
        }

        try {
            // Test successful ingestion submission
            val operation = actualWrapper.submitQueuedIngestion(
                database = database,
                table = table,
                blobUrls = testBlobUrls,
                format = format,
                ingestProperties = properties
            )
            
            logger.info("E2E Test Success: Submitted queued ingestion with operation ID: {}", operation.id)
            
            assertNotNull(operation, "IngestionOperation should not be null")
            assertNotNull(operation.id, "Operation ID should not be null")
            assertEquals(IngestionMethod.QUEUED, operation.ingestionMethod)
            assertEquals(database, operation.database)
            assertEquals(table, operation.table)
            
            // Start with lightweight summary check for efficient polling
            var summaryOperation = actualWrapper.getIngestionSummary(
                database = database,
                table = table,
                operationId = operation.id
            )
            
            logger.info("E2E Test Success: Retrieved initial ingestion summary for operation: {}", operation.id)
            
            // Wait for ingestion to complete with polling using summary API for efficiency
            val maxWaitTimeMs = 300_000L // 5 minutes maximum wait
            val pollIntervalMs = 5_000L   // Poll every 5 seconds
            val startTime = System.currentTimeMillis()
            
            while (System.currentTimeMillis() - startTime < maxWaitTimeMs) {
                val inProgress = summaryOperation.statusCounts?.inProgress ?: 0
                
                if (inProgress == 0) {
                    // Ingestion completed (either succeeded or failed)
                    logger.info("Ingestion completed after {} ms", System.currentTimeMillis() - startTime)
                    break
                }
                
                logger.info("Ingestion still in progress, waiting {} seconds before next check...", pollIntervalMs / 1000)
                kotlinx.coroutines.delay(pollIntervalMs)
                
                // Get updated summary (lightweight check during polling)
                summaryOperation = actualWrapper.getIngestionSummary(
                    database = database,
                    table = table,
                    operationId = operation.id
                )
            }
            
            // Check if we need detailed information (only if there are failures)
            val statusOperation = if (summaryOperation.statusCounts?.failed?.let { it > 0 } == true) {
                logger.info("Failures detected, retrieving detailed status for diagnosis...")
                actualWrapper.getIngestionDetails(
                    database = database,
                    table = table,
                    operationId = operation.id
                )
            } else {
                logger.info("No failures detected, using summary information")
                summaryOperation
            }
            
            // Log final detailed ingestion status information
            logger.info("=== FINAL INGESTION STATUS DETAILS ===")
            logger.info("Operation ID: {}", statusOperation.id)
            logger.info("Start Time: {}", statusOperation.startTime)
            
            val statusCounts2 = statusOperation.statusCounts
            if (statusCounts2 != null) {
                logger.info("Final Status Counts:")
                logger.info("  - Succeeded: {}", statusCounts2.succeeded ?: 0)
                logger.info("  - Failed: {}", statusCounts2.failed ?: 0)
                logger.info("  - In Progress: {}", statusCounts2.inProgress ?: 0)
                logger.info("  - Canceled: {}", statusCounts2.canceled ?: 0)
                
                val totalCompleted = (statusCounts2.succeeded ?: 0) + (statusCounts2.failed ?: 0) + (statusCounts2.canceled ?: 0)
                val totalInProgress = statusCounts2.inProgress ?: 0
                
                if (totalInProgress > 0) {
                    logger.warn("Ingestion timed out after {} ms with {} blobs still in progress", maxWaitTimeMs, totalInProgress)
                } else {
                    logger.info("Ingestion completed: {} blobs processed", totalCompleted)
                }
            }
            
            if (statusOperation.storedResults.isNotEmpty()) {
                logger.info("Detailed Results:")
                statusOperation.storedResults.forEachIndexed { index: Int, result: BlobStatus ->
                    logger.info("  Blob {}: Status={}, StartedAt={}, LastUpdateTime={}", 
                        index + 1, result.status, result.startedAt, result.lastUpdateTime)
                    if (result.details != null) {
                        logger.info("    Details: {}", result.details)
                    }
                    if (result.errorCode != null) {
                        logger.info("    Error Code: {}", result.errorCode)
                    }
                    if (result.failureStatus != null) {
                        logger.info("    Failure Status: {}", result.failureStatus)
                    }
                }
            }
            
            logger.info("=== END FINAL INGESTION STATUS ===")
            
            assertNotNull(statusOperation, "Status operation should not be null")
            assertEquals(operation.id, statusOperation.id)
            assertEquals(IngestionMethod.QUEUED, statusOperation.ingestionMethod)
            assertEquals(database, statusOperation.database)
            assertEquals(table, statusOperation.table)
            
            // Assert that ingestion succeeded
            val statusCounts = statusOperation.statusCounts
            if (statusCounts != null) {
                val succeeded = statusCounts.succeeded ?: 0
                val failed = statusCounts.failed ?: 0
                val totalBlobs = testBlobUrls.size
                
                assertEquals(totalBlobs, succeeded, "Expected $totalBlobs blobs to succeed, but only $succeeded succeeded. Failed: $failed")
                assertEquals(0, failed, "Expected no failed blobs, but $failed failed")
                
                if (failed > 0) {
                    // Build failure details manually
                    val failureDetailsList = ArrayList<String>()
                    statusOperation.storedResults.forEachIndexed { index: Int, result: BlobStatus ->
                        if (result.status == BlobStatus.Status.Failed) {
                            val errorInfo = result.details ?: result.failureStatus ?: "Unknown error"
                            failureDetailsList.add("Blob ${index + 1}: $errorInfo")
                        }
                    }
                    val failureDetails: String = failureDetailsList.joinToString("\n")
                    fail("Ingestion failed with errors:\n$failureDetails")
                }
            } else {
                fail("Status counts should not be null")
            }
            
        } catch (e: ConnectException) {
            // Skip test if we can't connect to the test cluster due to network issues
            assumeTrue(false, "Skipping test: Unable to connect to test cluster due to network connectivity issues: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: Unable to connect to test cluster due to network connectivity issues: ${e.cause?.message}")
            } else {
                throw e
            }
        }
    }

    @Test
    fun `test queued ingestion with inaccessible blob URL should fail`(): Unit = runBlocking {
        // Skip test if no DM_CONNECTION_STRING is set
        val cluster = System.getenv("DM_CONNECTION_STRING")
        if (cluster == null) {
            assumeTrue(false, "Skipping test: No DM_CONNECTION_STRING environment variable set for real cluster testing")
            return@runBlocking
        }

        val actualTokenProvider = AzCliTokenCredentialsProvider()
        val actualWrapper = QueuedIngestionApiWrapper(
            dmUrl = cluster,
            tokenCredentialsProvider = actualTokenProvider,
            skipSecurityChecks = true
        )

        // Test parameters with inaccessible blob URL
        val database = System.getenv("TEST_DATABASE") ?: "e2e"
        val table = System.getenv("TEST_TABLE") ?: "CovidTracking"
        val inaccessibleBlobUrls = listOf(
            "https://nonexistentaccount.blob.core.windows.net/container/file.csv"
        )
        val format = DataFormat.CSV
        val properties = ingestProperties {
            enableTracking(true)
        }

        try {
            // Submit ingestion - this might succeed but fail during processing
            val operation = actualWrapper.submitQueuedIngestion(
                database = database,
                table = table,
                blobUrls = inaccessibleBlobUrls,
                format = format,
                ingestProperties = properties
            )
            
            logger.info("E2E Negative Test: Submitted ingestion with inaccessible blob, operation ID: {}", operation.id)
            
            // Wait for ingestion to complete and expect it to fail
            val maxWaitTimeMs = 60_000L // 1 minute wait for failure
            val pollIntervalMs = 5_000L   // Poll every 5 seconds
            val startTime = System.currentTimeMillis()
            
            var statusOperation = operation
            while (System.currentTimeMillis() - startTime < maxWaitTimeMs) {
                statusOperation = actualWrapper.getIngestionSummary(
                    database = database,
                    table = table,
                    operationId = operation.id
                )
                
                val inProgress = statusOperation.statusCounts?.inProgress ?: 0
                val failed = statusOperation.statusCounts?.failed ?: 0
                
                if (inProgress == 0 || failed > 0) {
                    // Ingestion completed or failed
                    break
                }
                
                logger.info("Waiting for ingestion failure, still in progress...")
                kotlinx.coroutines.delay(pollIntervalMs)
            }
            
            // Verify that ingestion failed as expected
            val statusCounts = statusOperation.statusCounts
            assertNotNull(statusCounts, "Status counts should not be null")
            
            val failed = statusCounts.failed ?: 0
            val succeeded = statusCounts.succeeded ?: 0
            
            logger.info("E2E Negative Test: Final status - Failed: {}, Succeeded: {}", failed, succeeded)
            
            assertTrue(failed > 0, "Expected at least one blob to fail due to inaccessible URL, but failed count was $failed")
            assertEquals(0, succeeded, "Expected no blobs to succeed with inaccessible URL, but $succeeded succeeded")
            
            logger.info("E2E Negative Test Success: Inaccessible blob correctly failed during ingestion")
            
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: Unable to connect to test cluster due to network connectivity issues: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: Unable to connect to test cluster due to network connectivity issues: ${e.cause?.message}")
            } else {
                throw e
            }
        }
    }

    @Test
    fun `test queued ingestion with empty blob list should fail`(): Unit = runBlocking {
        // Skip test if no DM_CONNECTION_STRING is set
        val cluster = System.getenv("DM_CONNECTION_STRING")
        if (cluster == null) {
            assumeTrue(false, "Skipping test: No DM_CONNECTION_STRING environment variable set for real cluster testing")
            return@runBlocking
        }

        val actualTokenProvider = AzCliTokenCredentialsProvider()
        val actualWrapper = QueuedIngestionApiWrapper(
            dmUrl = cluster,
            tokenCredentialsProvider = actualTokenProvider,
            skipSecurityChecks = true
        )

        // Test parameters with empty blob list
        val database = System.getenv("TEST_DATABASE") ?: "e2e"
        val table = System.getenv("TEST_TABLE") ?: "CovidTracking"
        val emptyBlobUrls = emptyList<String>()
        val format = DataFormat.CSV
        val properties = ingestProperties {
            enableTracking(true)
        }

        try {
            // This should fail due to empty blob list
            val exception = assertThrows(Exception::class.java) {
                runBlocking {
                    actualWrapper.submitQueuedIngestion(
                        database = database,
                        table = table,
                        blobUrls = emptyBlobUrls,
                        format = format,
                        ingestProperties = properties
                    )
                }
            }
            
            logger.info("E2E Negative Test Success: Empty blob list correctly rejected with error: {}", exception.message)
            assertTrue(exception.message?.contains("400", ignoreCase = true) == true 
                    || exception.message?.contains("failed", ignoreCase = true) == true
                    || exception.message?.contains("submit", ignoreCase = true) == true,
                "Error message should indicate submission failure but was: ${exception.message}")
            
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: Unable to connect to test cluster due to network connectivity issues: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: Unable to connect to test cluster due to network connectivity issues: ${e.cause?.message}")
            } else {
                throw e
            }
        }
    }
}
