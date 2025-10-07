// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.net.ConnectException
import kotlin.test.assertNotNull
import kotlin.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
class QueuedIngestionClientTest :
    IngestV2TestBase(QueuedIngestionClientTest::class.java) {
    override val columnNamesToTypes =
        mapOf(
            "StartTime" to "datetime",
            "EndTime" to "datetime",
            "EpisodeId" to "string",
            "EventId" to "string",
            "State" to "string",
            "EventType" to "string",
            "InjuriesDirect" to "long",
            "InjuriesIndirect" to "long",
            "DeathsDirect" to "long",
            "DeathsIndirect" to "long",
            "DamageProperty" to "long",
            "DamageCrops" to "long",
            "Source" to "string",
            "BeginLocation" to "string",
            "EndLocation" to "string",
            "BeginLat" to "real",
            "BeginLon" to "real",
            "EndLat" to "real",
            "EndLon" to "real",
            "EpisodeNarrative" to "string",
            "EventNarrative" to "string",
            "StormSummary" to "dynamic",
            "SourceLocation" to "string",
            "Type" to "string",
        )

    @ParameterizedTest(name = "[QueuedIngestion] {index} => TestName ={0}")
    @CsvSource(
        "QueuedIngestion-NoMapping,https://kustosamples.blob.core.windows.net/samplefiles/StormEvents.csv,false,false,0",
        // TODO This test fails (ingestionMappingReference is not passed correctly)
        // "QueuedIngestion-WithMappingReference,https://kustosamples.blob.core.windows.net/samplefiles/StormEvents.csv,true,false,0",
        "QueuedIngestion-WithInlineMapping,https://kustosamples.blob.core.windows.net/samplefiles/StormEvents.csv,false,true,0",
        // TODO This test fails (failureStatus is not right)
        // "QueuedIngestion-FailWithInvalidBlob,https://nonexistentaccount.blob.core.windows.net/samplefiles/StormEvents.json,false,false,0",
        //  "https://nonexistentaccount.blob.core.windows.net/samplefiles/StormEvents.json, 1",

    )
    fun `test queued ingestion with CSV blob`(
        testName: String,
        blobUrl: String,
        mappingReference: Boolean,
        ingestionMapping: Boolean,
        numberOfFailures: Int,
    ): Unit = runBlocking {
        // Skip test if no DM_CONNECTION_STRING is set
        logger.info("Starting test: $testName")
        val queuedIngestionClient =
            QueuedIngestionClient(
                dmUrl = dmEndpoint,
                tokenCredential = tokenProvider,
                skipSecurityChecks = true,
            )
        val testBlobUrls = listOf(blobUrl)
        val format = Format.csv

        val properties =
            if (mappingReference) {
                IngestRequestProperties(
                    format = Format.csv,
                    ignoreFirstRecord = true,
                    mappingReference = "${targetTable}_mapping",
                    enableTracking = true,
                )
            } else if (ingestionMapping) {
                val ingestionMappingArray =
                    columnNamesToTypes.keys.mapIndexed { idx, col ->
                        when (col) {
                            "SourceLocation" ->
                                mapOf(
                                    "Column" to col,
                                    "Properties" to
                                        mapOf(
                                            "Transform" to
                                                "SourceLocation",
                                        ),
                                )
                            "Type" ->
                                mapOf(
                                    "Column" to col,
                                    "Properties" to
                                        mapOf(
                                            "ConstValue" to
                                                "IngestionMapping",
                                        ),
                                )
                            else ->
                                mapOf(
                                    "Column" to col,
                                    "Properties" to
                                        mapOf(
                                            "Ordinal" to
                                                idx
                                                    .toString(),
                                        ),
                                )
                        }
                    }
                IngestRequestProperties(
                    format = Format.csv,
                    ignoreFirstRecord = true,
                    mapping = ingestionMappingArray.toString(),
                    enableTracking = true,
                )
            } else {
                IngestRequestProperties(
                    format = Format.csv,
                    ignoreFirstRecord = true,
                    enableTracking = true,
                )
            }

        try {
            // Test successful ingestion submission
            val ingestionResponse =
                queuedIngestionClient.submitQueuedIngestion(
                    database = database,
                    table = targetTable,
                    blobUrls = testBlobUrls,
                    format = format,
                    ingestProperties = properties,
                )

            logger.info(
                "E2E: Submitted queued ingestion with operation ID: {}",
                ingestionResponse.ingestionOperationId,
            )
            assertNotNull(
                ingestionResponse,
                "IngestionOperation should not be null",
            )
            assertNotNull(
                ingestionResponse.ingestionOperationId,
                "Operation ID should not be null",
            )
            // Test polling until completion with timeout
            logger.info(
                "Starting to poll for completion of operation: {}",
                ingestionResponse.ingestionOperationId,
            )

            val finalStatus =
                queuedIngestionClient.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId =
                    ingestionResponse.ingestionOperationId,
                    // Poll every 5 seconds for testing
                    pollingInterval = Duration.parse("PT5S"),
                    // 5 minute timeout for testing
                    timeout = Duration.parse("PT5M"),
                )

            logger.info(
                "Ingestion completed with final status: {}",
                finalStatus.status,
            )

            // Verify the operation completed successfully
            // Check if we have any results
            if (finalStatus.details?.isNotEmpty() == true) {
                val succeededCount =
                    finalStatus.details.count {
                        it.status == BlobStatus.Status.Succeeded
                    }
                val failedCount =
                    finalStatus.details.count {
                        it.status == BlobStatus.Status.Failed
                    }
                logger.info(
                    "Ingestion results - Succeeded: {}, Failed: {}",
                    succeededCount,
                    failedCount,
                )
                // For this test, we expect at least some processing to have occurred
                assert(succeededCount > 0 || failedCount > 0) {
                    "Expected at least some blobs to be processed"
                }

                assert(failedCount == numberOfFailures) {
                    "Expected $numberOfFailures failed ingestions, but got $failedCount"
                }

                if (failedCount > 0) {
                    finalStatus.details
                        .filter { blobStatus ->
                            blobStatus.status == BlobStatus.Status.Failed
                        }
                        .forEach { failedBlob ->
                            logger.error(
                                "Blob ingestion failed for sourceId: ${failedBlob.sourceId}, message: ${failedBlob.details}",
                            )
                        }
                    logger.error(
                        "There are $failedCount blobs that failed ingestion.",
                    )
                }
                val filterType =
                    when {
                        mappingReference -> "MappingRef"
                        ingestionMapping -> "IngestionMapping"
                        else -> "None"
                    }
                if (mappingReference) {
                    val results =
                        adminClient
                            .executeQuery(
                                database,
                                "$targetTable | where Type == '$filterType' | summarize count=count() by SourceLocation",
                            )
                            .primaryResults
                    assertNotNull(results, "Query results should not be null")
                    results.next()
                    val count: Long = results.getLong("count")
                    assertNotNull(count, "Count should not be null")
                    assert(count > 0) {
                        "Expected some records in the table after ingestion"
                    }
                    val sourceLocation: String =
                        results.getString("SourceLocation")
                    assert(sourceLocation.isNotEmpty()) {
                        "SourceLocation should not be empty"
                    }
                }
            }
        } catch (e: ConnectException) {
            // Skip test if we can't connect to the test cluster due to network issues
            assumeTrue(
                false,
                "Skipping test: Unable to connect to test cluster due to network connectivity issues: ${e.message}",
            )
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(
                    false,
                    "Skipping test: Unable to connect to test cluster due to network connectivity issues: ${e.cause?.message}",
                )
            } else {
                throw e
            }
        }
    }
}
