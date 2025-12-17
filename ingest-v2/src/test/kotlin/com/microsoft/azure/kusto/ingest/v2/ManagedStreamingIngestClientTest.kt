// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.builders.ManagedStreamingIngestClientBuilder
import com.microsoft.azure.kusto.ingest.v2.client.policy.DefaultManagedStreamingPolicy
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.io.ByteArrayInputStream
import java.net.ConnectException
import java.time.Duration
import java.util.*
import kotlin.test.DefaultAsserter.assertNotNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * End-to-end tests for ManagedStreamingIngestClient.
 *
 * These tests verify that the client correctly:
 * 1. Attempts streaming ingestion for small data
 * 2. Falls back to queued ingestion for large data or when streaming fails
 * 3. Handles various error scenarios
 * 4. Respects the managed streaming policy settings
 */
@Execution(ExecutionMode.CONCURRENT)
class ManagedStreamingIngestClientTest :
    IngestV2TestBase(ManagedStreamingIngestClientTest::class.java) {

    private val targetUuid = UUID.randomUUID().toString()
    private val randomRow: String =
        """{"timestamp": "2023-05-02 15:23:50.0000000","deviceId": "$targetUuid","messageId": "7f316225-839a-4593-92b5-1812949279b3","temperature": 31.0301639051317,"humidity": 62.0791099602725}"""
            .trimIndent()

    private val managedClient =
        ManagedStreamingIngestClientBuilder.create(dmUrl = dmEndpoint)
            .withAuthentication(tokenProvider)
            .withManagedStreamingIngestPolicy(
                DefaultManagedStreamingPolicy(),
            )
            .build()

    /** Test managed streaming ingestion with small blob data */
    @ParameterizedTest(
        name = "[ManagedStreaming-SmallData] {index} => TestName={0}",
    )
    @CsvSource(
        "ManagedStreaming-SmallBlob,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json,json",
        "ManagedStreaming-SmallMultilineBlob,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/multilined.json,multijson",
    )
    fun `test managed streaming ingestion with small blob data`(
        testName: String,
        blobUrl: String,
        targetFormat: String,
    ): Unit = runBlocking {
        logger.info("Starting test: $testName")
        val format =
            when (targetFormat.lowercase()) {
                "json" -> Format.json
                "multijson" -> Format.multijson
                else ->
                    throw IllegalArgumentException(
                        "Unsupported format: $targetFormat",
                    )
            }
        val testSources = BlobSource(blobUrl, format = format)
        val ingestRequestProperties =
            IngestRequestPropertiesBuilder.create(database, targetTable)
                .withEnableTracking(true)
                .build()
        try {
            // Ingest data - should attempt streaming first
            val ingestionResponse =
                managedClient.ingestAsync(
                    source = testSources,
                    ingestRequestProperties = ingestRequestProperties,
                )
            logger.info(
                "E2E: Submitted managed streaming ingestion with operation ID: {}",
                ingestionResponse.ingestResponse.ingestionOperationId,
            )
            assertNotNull(
                ingestionResponse,
                "IngestionOperation should not be null",
            )
            assertNotNull(
                ingestionResponse.ingestResponse.ingestionOperationId,
                "Operation ID should not be null",
            )

            // If it fell back to queued ingestion, poll for status
            if (ingestionResponse.ingestionType == IngestKind.QUEUED) {
                logger.info(
                    "Ingestion fell back to queued mode. Polling for completion...",
                )
                val finalStatus =
                    managedClient.pollUntilCompletion(
                        database = database,
                        table = targetTable,
                        operationId =
                        ingestionResponse.ingestResponse
                            .ingestionOperationId!!,
                        pollingInterval = Duration.parse("PT5S"),
                        timeout = Duration.parse("PT5M"),
                    )

                logger.info(
                    "Ingestion completed with final status: {}",
                    finalStatus.status,
                )

                assert(
                    finalStatus.details?.any {
                        it.status == BlobStatus.Status.Succeeded
                    } == true,
                ) {
                    "Expected at least one successful ingestion"
                }
            } else {
                // Streaming ingestion - verify data was ingested
                logger.info("Ingestion used streaming mode. Verifying data...")

                // kotlinx.coroutines.delay(3000)
                awaitAndQuery(
                    query = "$targetTable | summarize count=count()",
                    expectedResultsCount = 5,
                )
            }
        } catch (e: ConnectException) {
            assumeTrue(
                false,
                "Skipping test: Unable to connect to test cluster: ${e.message}",
            )
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(
                    false,
                    "Skipping test: Unable to connect to test cluster: ${e.cause?.message}",
                )
            } else {
                throw e
            }
        }
    }

    /** Test managed streaming with custom policy */
    @ParameterizedTest(
        name = "[ManagedStreaming-CustomPolicy] {index} => TestName={0}",
    )
    @CsvSource(
        "CustomPolicy-ContinueWhenUnavailable,true,1.0",
        "CustomPolicy-ReducedSizeLimit,false,0.25",
    )
    fun `test managed streaming with custom policy`(
        testName: String,
        continueWhenUnavailable: Boolean,
        dataSizeFactor: Double,
    ) = runBlocking {
        logger.info("Starting custom policy test: $testName")
        alterTableToEnableStreaming()
        clearDatabaseSchemaCache()
        val customPolicy =
            DefaultManagedStreamingPolicy(
                continueWhenStreamingIngestionUnavailable =
                continueWhenUnavailable,
                dataSizeFactor = dataSizeFactor,
            )
        val customManagedClient =
            ManagedStreamingIngestClientBuilder.create(dmUrl = dmEndpoint)
                .withAuthentication(tokenProvider)
                .withManagedStreamingIngestPolicy(
                    DefaultManagedStreamingPolicy(),
                )
                .withManagedStreamingIngestPolicy(customPolicy)
                .build()
        val testData = randomRow
        val source =
            StreamSource(
                stream = ByteArrayInputStream(testData.toByteArray()),
                format = targetTestFormat,
                sourceCompression = CompressionType.NONE,
                sourceId = UUID.randomUUID(),
                name = "test-custom-policy",
            )

        val properties =
            IngestRequestPropertiesBuilder.create(database, targetTable)
                .withEnableTracking(true)
                .build()

        try {
            val ingestionResponse =
                customManagedClient.ingestAsync(
                    source = source,
                    ingestRequestProperties = properties,
                )
            assertNotNull(
                ingestionResponse,
                "Ingestion response should not be null",
            )
            // Verify data was ingested (either via streaming or queued)
            // If it used queued ingestion, poll for status
            if (ingestionResponse.ingestionType == IngestKind.QUEUED) {
                val finalStatus =
                    customManagedClient.pollUntilCompletion(
                        database = database,
                        table = targetTable,
                        operationId =
                        ingestionResponse.ingestResponse
                            .ingestionOperationId!!,
                        pollingInterval = Duration.parse("PT5S"),
                        timeout = Duration.parse("PT5M"),
                    )
                logger.info(
                    "{} ingestion fell back to QUEUED mode and completed with final status: {}",
                    testName,
                    finalStatus.status,
                )
                assertNotNull(finalStatus.status?.succeeded)
                finalStatus.status.succeeded.let {
                    assert(it > 0) {
                        "Expected at least one successful ingestion for $testName"
                    }
                }
            }
            awaitAndQuery(
                query =
                "$targetTable | where deviceId == '$targetUuid' | summarize count=count()",
                expectedResultsCount = 1,
            )
        } catch (e: ConnectException) {
            assumeTrue(
                false,
                "Skipping test: Unable to connect to test cluster: ${e.message}",
            )
        }
    }

    @Test
    fun fallbackToQueuedIngestionTest() = runBlocking {
        val customPolicy =
            DefaultManagedStreamingPolicy(
                continueWhenStreamingIngestionUnavailable = true,
                dataSizeFactor = 0.25,
            )
        val customManagedClient =
            ManagedStreamingIngestClientBuilder.create(dmUrl = dmEndpoint)
                .withAuthentication(tokenProvider)
                .withManagedStreamingIngestPolicy(
                    DefaultManagedStreamingPolicy(),
                )
                .withManagedStreamingIngestPolicy(customPolicy)
                .build()
        adminClusterClient.executeMgmt(
            database,
            ".alter table $targetTable policy streamingingestion disable",
        )
        clearDatabaseSchemaCache()
        val testSource =
            BlobSource(
                "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/multilined.json",
                format = Format.multijson,
            )
        val ingestRequestProperties =
            IngestRequestPropertiesBuilder.create(database, targetTable)
                .withEnableTracking(true)
                .build()
        val ingestionResponse =
            customManagedClient.ingestAsync(
                source = testSource,
                ingestRequestProperties = ingestRequestProperties,
            )
        assertNotNull(
            ingestionResponse,
            "Ingestion response should not be null",
        )
        assertEquals(
            IngestKind.QUEUED,
            ingestionResponse.ingestionType,
            "Ingestion should have fallen back to QUEUED",
        )
        awaitAndQuery(
            query = "$targetTable | summarize count=count()",
            expectedResultsCount = 5,
        )
    }
}
