// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.source.AbstractSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.azure.kusto.ingest.v2.source.FileSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.StreamSourceInfo
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import java.io.ByteArrayInputStream
import java.net.ConnectException
import java.nio.file.Files
import java.util.UUID
import java.util.stream.Stream
import kotlin.test.assertNotNull
import kotlin.time.Duration

/**
 * End-to-end tests for ManagedStreamingIngestClient.
 *
 * These tests verify that the client correctly:
 * 1. Attempts streaming ingestion for small data
 * 2. Falls back to queued ingestion for large data or when streaming fails
 * 3. Handles various error scenarios
 * 4. Respects the managed streaming policy settings
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
class ManagedStreamingIngestClientTest :
    IngestV2TestBase(ManagedStreamingIngestClientTest::class.java) {

    private val publicBlobUrl =
        "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json"

    private val targetUuid = UUID.randomUUID().toString()
    private val randomRow: String =
        """{"timestamp": "2023-05-02 15:23:50.0000000","deviceId": "$targetUuid","messageId": "7f316225-839a-4593-92b5-1812949279b3","temperature": 31.0301639051317,"humidity": 62.0791099602725}"""
            .trimIndent()

    /** Test managed streaming ingestion with small blob data */
    @ParameterizedTest(
        name = "[ManagedStreaming-SmallData] {index} => TestName={0}",
    )
    @CsvSource(
        "ManagedStreaming-SmallBlob,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json",
        "ManagedStreaming-SmallMultilineBlob,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/multilined.json",
    )
    fun `test managed streaming ingestion with small blob data`(
        testName: String,
        blobUrl: String,
    ): Unit = runBlocking {
        logger.info("Starting test: $testName")
        val managedClient =
            ManagedStreamingIngestClient(
                clusterUrl = engineEndpoint,
                tokenCredential = tokenProvider,
                managedStreamingPolicy =
                DefaultManagedStreamingPolicy(),
                skipSecurityChecks = true,
            )

        val testSources = listOf(BlobSourceInfo(blobUrl))
        val properties =
            IngestRequestProperties(
                format = targetTestFormat,
                enableTracking = true,
            )

        try {
            // Ingest data - should attempt streaming first
            val ingestionResponse =
                managedClient.submitManagedIngestion(
                    database = database,
                    table = targetTable,
                    sources = testSources,
                    format = targetTestFormat,
                    ingestProperties = properties,
                )

            logger.info(
                "E2E: Submitted managed streaming ingestion with operation ID: {}",
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

            // If it fell back to queued ingestion, poll for status
            if (
                !ingestionResponse.ingestionOperationId.startsWith(
                    "managed-",
                )
            ) {
                logger.info(
                    "Ingestion fell back to queued mode. Polling for completion...",
                )
                val finalStatus =
                    managedClient.pollUntilCompletion(
                        database = database,
                        table = targetTable,
                        operationId =
                        ingestionResponse.ingestionOperationId,
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
                kotlinx.coroutines.delay(3000)

                val results =
                    adminClusterClient
                        .executeQuery(
                            database,
                            "$targetTable | summarize count=count()",
                        )
                        .primaryResults

                assertNotNull(results, "Query results should not be null")
                results.next()
                val count: Long = results.getLong("count")
                assertNotNull(count, "Count should not be null")
                assert(count > 0) {
                    "Expected records in table after streaming ingestion, but got $count"
                }
                logger.info(
                    "Streaming ingestion verified - {} records in table",
                    count,
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

    /** Test managed streaming with small streaming data */
    @ParameterizedTest(
        name = "[ManagedStreaming-DirectData] {index} => TestName={0}",
    )
    @MethodSource("directDataTestParameters")
    fun `test managed streaming with small stream data`(
        testName: String,
        data: String,
        deviceId: String,
    ) = runBlocking {
        logger.info(
            "Starting managed streaming with small stream data: $testName",
        )

        val managedClient =
            ManagedStreamingIngestClient(
                clusterUrl = engineEndpoint,
                tokenCredential = tokenProvider,
                managedStreamingPolicy =
                DefaultManagedStreamingPolicy(),
                skipSecurityChecks = true,
            )

        val source =
            StreamSourceInfo(
                stream = ByteArrayInputStream(data.toByteArray()),
                format = targetTestFormat,
                sourceCompression = CompressionType.NONE,
                sourceId = UUID.randomUUID(),
                name = "test-stream",
            )

        val properties =
            IngestRequestProperties(
                format = targetTestFormat,
                enableTracking = true,
            )

        try {
            val ingestionResponse =
                managedClient.submitManagedIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(source),
                    format = targetTestFormat,
                    ingestProperties = properties,
                )

            kotlinx.coroutines.delay(5000)

            val results =
                adminClusterClient
                    .executeQuery(
                        database,
                        "$targetTable | where deviceId == '$deviceId' | summarize count=count() by deviceId",
                    )
                    .primaryResults
            assertNotNull(
                ingestionResponse,
                "IngestionOperation should not be null",
            )
            assertNotNull(results, "Query results should not be null")
            results.next()
            val count: Long = results.getLong("count")
            assertNotNull(count, "Count should not be null")
            assert(count == 1L) {
                "Expected 1 record for $deviceId, but got $count"
            }
            logger.debug("{} verified successfully", testName)
        } catch (e: ConnectException) {
            assumeTrue(
                false,
                "Skipping test: Unable to connect to test cluster: ${e.message}",
            )
        }
    }

    private fun directDataTestParameters(): Stream<Arguments> {
        val directDataId = UUID.randomUUID().toString()
        val directData =
            """{"timestamp": "2023-05-02 15:23:50.0000000","deviceId": "$directDataId","messageId": "test-message-1","temperature": 25.5,"humidity": 60.0}"""
        return Stream.of(
            Arguments.of("DirectData-SingleRow", directData, directDataId),
        )
    }

    /** Test managed streaming with multiple sources (file and stream) */
    @ParameterizedTest(
        name =
        "[ManagedStreaming-LocalSource] {index} => SourceType={0}, TestName={1}",
    )
    @CsvSource(
        "file,ManagedStreaming-FileSource,SampleFileSource.json",
        "stream,ManagedStreaming-StreamSource,SampleStreamSource.json",
    )
    fun `test managed streaming with multiple sources`(
        sourceType: String,
        testName: String,
        fileName: String,
    ) = runBlocking {
        logger.info("Starting multiple sources test: $testName")

        // Download test data
        val deviceDataUrl =
            "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/multilined.json"
        val deviceData = java.net.URL(deviceDataUrl).readText()
        val targetFormat = Format.multijson

        val source: AbstractSourceInfo =
            when (sourceType) {
                "file" -> {
                    val tempFile = Files.createTempFile(fileName, null)
                    Files.write(tempFile, deviceData.toByteArray())
                    FileSourceInfo(
                        path = tempFile,
                        format = targetFormat,
                        compressionType = CompressionType.NONE,
                        name = fileName,
                        sourceId = UUID.randomUUID(),
                    )
                        .also {
                            Runtime.getRuntime()
                                .addShutdownHook(
                                    Thread {
                                        Files.deleteIfExists(
                                            tempFile,
                                        )
                                    },
                                )
                        }
                }
                "stream" ->
                    StreamSourceInfo(
                        stream =
                        ByteArrayInputStream(
                            deviceData.toByteArray(),
                        ),
                        format = targetFormat,
                        sourceCompression = CompressionType.NONE,
                        sourceId = UUID.randomUUID(),
                        name = fileName,
                    )
                else -> error("Unknown sourceType: $sourceType")
            }

        val managedClient =
            ManagedStreamingIngestClient(
                clusterUrl = engineEndpoint,
                tokenCredential = tokenProvider,
                managedStreamingPolicy =
                DefaultManagedStreamingPolicy(),
                skipSecurityChecks = true,
            )

        val properties =
            IngestRequestProperties(
                format = targetFormat,
                enableTracking = true,
            )

        val ingestionResponse =
            managedClient.submitManagedIngestion(
                database = database,
                table = targetTable,
                sources = listOf(source),
                format = targetFormat,
                ingestProperties = properties,
            )

        assertNotNull(
            ingestionResponse,
            "IngestionOperation should not be null",
        )
        assertNotNull(
            ingestionResponse.ingestionOperationId,
            "Operation ID should not be null",
        )

        // If it used queued ingestion, poll for status
        if (!ingestionResponse.ingestionOperationId.startsWith("managed-")) {
            val finalStatus =
                managedClient.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId =
                    ingestionResponse.ingestionOperationId,
                    pollingInterval = Duration.parse("PT5S"),
                    timeout = Duration.parse("PT5M"),
                )

            logger.info(
                "{} ingestion completed with final status: {}",
                testName,
                finalStatus.status,
            )

            assert(
                finalStatus.details?.any {
                    it.status == BlobStatus.Status.Succeeded
                } == true,
            ) {
                "Expected at least one successful ingestion for $testName"
            }
        }
    }

    /** Test managed streaming with custom policy */
    @ParameterizedTest(
        name = "[ManagedStreaming-CustomPolicy] {index} => TestName={0}",
    )
    @CsvSource(
        "CustomPolicy-ContinueWhenUnavailable,true,1.0",
        "CustomPolicy-ReducedSizeLimit,false,0.5",
    )
    fun `test managed streaming with custom policy`(
        testName: String,
        continueWhenUnavailable: Boolean,
        dataSizeFactor: Double,
    ) = runBlocking {
        logger.info("Starting custom policy test: $testName")

        val customPolicy =
            DefaultManagedStreamingPolicy(
                continueWhenStreamingIngestionUnavailable =
                continueWhenUnavailable,
                dataSizeFactor = dataSizeFactor,
            )

        val managedClient =
            ManagedStreamingIngestClient(
                clusterUrl = engineEndpoint,
                tokenCredential = tokenProvider,
                managedStreamingPolicy = customPolicy,
                skipSecurityChecks = true,
            )

        val testData = randomRow
        val source =
            StreamSourceInfo(
                stream = ByteArrayInputStream(testData.toByteArray()),
                format = targetTestFormat,
                sourceCompression = CompressionType.NONE,
                sourceId = UUID.randomUUID(),
                name = "test-custom-policy",
            )

        val properties =
            IngestRequestProperties(
                format = targetTestFormat,
                enableTracking = true,
            )

        try {
            val ingestionResponse =
                managedClient.submitManagedIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(source),
                    format = targetTestFormat,
                    ingestProperties = properties,
                )

            assertNotNull(
                ingestionResponse,
                "Ingestion response should not be null",
            )

            // Verify data was ingested (either via streaming or queued)
            kotlinx.coroutines.delay(5000)

            val results =
                adminClusterClient
                    .executeQuery(
                        database,
                        "$targetTable | where deviceId == '$targetUuid' | summarize count=count()",
                    )
                    .primaryResults

            assertNotNull(results, "Query results should not be null")
            if (results.next()) {
                val count: Long = results.getLong("count")
                logger.info("{} ingested {} records", testName, count)
                // We verify data was ingested regardless of method
                assert(count > 0) {
                    "Expected data to be ingested with custom policy"
                }
            }
        } catch (e: ConnectException) {
            assumeTrue(
                false,
                "Skipping test: Unable to connect to test cluster: ${e.message}",
            )
        }
    }
}
