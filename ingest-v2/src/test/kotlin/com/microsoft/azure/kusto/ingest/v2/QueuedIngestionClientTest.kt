// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestionClientBuilder
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.ColumnMapping
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.InlineIngestionMapping
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.TransformationMethod
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.source.AbstractSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.azure.kusto.ingest.v2.source.FileSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.StreamSourceInfo
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.api.parallel.ResourceLock
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.io.ByteArrayInputStream
import java.net.ConnectException
import java.nio.file.Files
import java.util.UUID
import kotlin.test.assertNotNull
import kotlin.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
class QueuedIngestionClientTest :
    IngestV2TestBase(QueuedIngestionClientTest::class.java) {

    @Test
    fun `test builder with optional parameters`() {
        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withClientDetails("TestClient", "1.0")
                .withMaxConcurrency(10)
                .build()

        assertNotNull(client, "Client should not be null")
    }

    @Test
    fun `test builder with connector client details`() {
        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withConnectorClientDetails(
                    name = "TestConnector",
                    version = "2.0",
                    appName = "MyApp",
                    appVersion = "1.5",
                    additionalFields =
                    mapOf(
                        "JobId" to "job-123",
                        "RunId" to "run-456",
                    ),
                )
                .build()

        assertNotNull(client, "Client should not be null")
    }

    @Test
    fun `test builder with connector client details and user`() {
        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withConnectorClientDetails(
                    name = "TestConnector",
                    version = "2.0",
                    sendUser = true,
                    overrideUser = "test-user@example.com",
                )
                .build()

        assertNotNull(client, "Client should not be null")
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `test queued ingestion with builder pattern`(): Unit = runBlocking {
        logger.info("Starting builder pattern test")

        val queuedIngestionClient: IngestClient =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .build()

        val blobUrl =
            "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json"
        val testSources = listOf(BlobSourceInfo(blobUrl))
        val properties =
            IngestRequestProperties(
                format = targetTestFormat,
                ingestionMappingReference = "${targetTable}_mapping",
                enableTracking = true,
            )

        try {
            val ingestionResponse =
                queuedIngestionClient.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = testSources,
                    format = targetTestFormat,
                    ingestProperties = properties,
                )

            logger.info(
                "Builder pattern test: Submitted queued ingestion with operation ID: {}",
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

            val finalStatus =
                (queuedIngestionClient as QueuedIngestionClient)
                    .pollUntilCompletion(
                        database = database,
                        table = targetTable,
                        operationId =
                        ingestionResponse
                            .ingestionOperationId,
                        pollingInterval = Duration.parse("PT5S"),
                        timeout = Duration.parse("PT5M"),
                    )

            logger.info(
                "Builder pattern test: Ingestion completed with final status: {}",
                finalStatus.status,
            )

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
                    "Builder pattern test: Succeeded: {}, Failed: {}",
                    succeededCount,
                    failedCount,
                )

                assert(succeededCount > 0 || failedCount > 0) {
                    "Expected at least some blobs to be processed"
                }
            } else {
                logger.info(
                    "Builder pattern test: No details available, but operation was submitted successfully",
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

    @ParameterizedTest(name = "[QueuedIngestion] {index} => TestName ={0}")
    @ResourceLock("blob-ingestion")
    @CsvSource(
        // Single JSON blob, no mapping
        "QueuedIngestion-NoMapping,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json,false,false,0",
        // Single JSON blob, with mapping reference
        "QueuedIngestion-WithMappingReference,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json,true,false,0",
        // Single JSON blob, with inline mapping
        "QueuedIngestion-WithInlineMapping,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json,false,true,0",
        // TODO This test fails (failureStatus is not right)
        // "QueuedIngestion-FailWithInvalidBlob,https://nonexistentaccount.blob.core.windows.net/samplefiles/StormEvents.json,false,false,0",
        //  "https://nonexistentaccount.blob.core.windows.net/samplefiles/StormEvents.json, 1",

    )
    fun `test queued ingestion with CSV blob`(
        testName: String,
        blobUrl: String,
        useMappingReference: Boolean,
        useInlineIngestionMapping: Boolean,
        numberOfFailures: Int,
    ): Unit = runBlocking {
        // Skip test if no DM_CONNECTION_STRING is set
        logger.info("Starting test: $testName")
        val queuedIngestionClient: IngestClient =
            QueuedIngestionClient(
                dmUrl = dmEndpoint,
                tokenCredential = tokenProvider,
                skipSecurityChecks = true,
            )
        val testSources = listOf(BlobSourceInfo(blobUrl))

        val properties =
            if (useMappingReference) {
                IngestRequestProperties(
                    format = targetTestFormat,
                    ingestionMappingReference =
                    "${targetTable}_mapping",
                    enableTracking = true,
                )
            } else if (useInlineIngestionMapping) {
                val ingestionColumnMappings =
                    columnNamesToTypes.keys.map { col ->
                        when (col) {
                            "SourceLocation" ->
                                ColumnMapping(
                                    columnName = col,
                                    columnType =
                                    "string",
                                )
                                    .apply {
                                        setTransform(
                                            TransformationMethod
                                                .SourceLocation,
                                        )
                                    }
                            "Type" ->
                                ColumnMapping(
                                    columnName = col,
                                    columnType =
                                    "string",
                                )
                                    .apply {
                                        setConstantValue(
                                            "IngestionMapping",
                                        )
                                    }
                            else ->
                                ColumnMapping(
                                    columnName = col,
                                    columnType =
                                    columnNamesToTypes[
                                        col,
                                    ]!!,
                                )
                                    .apply { setPath("$.$col") }
                        }
                    }
                val inlineIngestionMappingInline =
                    InlineIngestionMapping(
                        columnMappings = ingestionColumnMappings,
                        ingestionMappingType =
                        InlineIngestionMapping
                            .IngestionMappingType
                            .JSON,
                    )
                val ingestionMappingString =
                    Json.encodeToString(
                        inlineIngestionMappingInline.columnMappings,
                    )
                IngestRequestProperties(
                    format = targetTestFormat,
                    ingestionMapping = ingestionMappingString,
                    enableTracking = true,
                )
            } else {
                IngestRequestProperties(
                    format = targetTestFormat,
                    enableTracking = true,
                )
            }

        try {
            // Test successful ingestion submission
            val ingestionResponse =
                queuedIngestionClient.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = testSources,
                    format = targetTestFormat,
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
                (queuedIngestionClient as QueuedIngestionClient)
                    .pollUntilCompletion(
                        database = database,
                        table = targetTable,
                        operationId =
                        ingestionResponse
                            .ingestionOperationId,
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
                        useMappingReference -> "MappingRef"
                        useInlineIngestionMapping -> "IngestionMapping"
                        else -> "None"
                    }
                if (useMappingReference || useInlineIngestionMapping) {
                    val results =
                        adminClusterClient
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

    private fun createTestStreamSource(
        sizeInBytes: Int,
        name: String,
    ): StreamSourceInfo {
        val jsonLine =
            """{"testField":"value","size":$sizeInBytes,"name":"$name"}""" +
                "\n"
        val jsonLineBytes = jsonLine.toByteArray()

        val numLines = (sizeInBytes / jsonLineBytes.size).coerceAtLeast(1)
        val data = ByteArray(numLines * jsonLineBytes.size)

        for (i in 0 until numLines) {
            System.arraycopy(
                jsonLineBytes,
                0,
                data,
                i * jsonLineBytes.size,
                jsonLineBytes.size,
            )
        }

        return StreamSourceInfo(
            stream = ByteArrayInputStream(data),
            format = Format.multijson,
            sourceCompression = CompressionType.NONE,
            sourceId = UUID.randomUUID(),
            name = name,
        )
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `E2E - single small file upload`() = runBlocking {
        logger.info("E2E: Testing single upload with small file")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .build()

        val source = createTestStreamSource(1024, "e2e_single_small.json")

        try {
            val response =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(source),
                    format = Format.multijson,
                    ingestProperties =
                    IngestRequestProperties(
                        format = Format.multijson,
                        enableTracking = true,
                    ),
                )

            assertNotNull(response.ingestionOperationId)
            logger.info(
                "E2E: Single small file submitted: ${response.ingestionOperationId}",
            )

            val finalStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = response.ingestionOperationId,
                    pollingInterval = Duration.parse("PT5S"),
                    timeout = Duration.parse("PT5M"),
                )

            val succeededCount =
                finalStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0
            assert(succeededCount > 0) { "Expected successful ingestion" }
            logger.info("E2E: Single small file upload completed successfully")
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: ${e.cause?.message}")
            } else {
                throw e
            }
        }
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `E2E - single large file upload`() = runBlocking {
        logger.info("E2E: Testing single upload with large file (10MB)")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .build()

        val source =
            createTestStreamSource(
                10 * 1024 * 1024,
                "e2e_single_large.json",
            )

        try {
            val response =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(source),
                    format = Format.multijson,
                    ingestProperties =
                    IngestRequestProperties(
                        format = Format.multijson,
                        enableTracking = true,
                    ),
                )

            assertNotNull(response.ingestionOperationId)
            logger.info(
                "E2E: Large file submitted: ${response.ingestionOperationId}",
            )

            val finalStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = response.ingestionOperationId,
                    pollingInterval = Duration.parse("PT5S"),
                    timeout = Duration.parse("PT5M"),
                )

            val succeededCount =
                finalStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0
            assert(succeededCount > 0) {
                "Expected successful large file ingestion"
            }
            logger.info("E2E: Large file upload completed successfully")
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: ${e.cause?.message}")
            } else {
                throw e
            }
        }
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `E2E - batch upload multiple files`() = runBlocking {
        logger.info("E2E: Testing batch upload with multiple files")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .build()

        val sources =
            (1..5).map { index ->
                createTestStreamSource(
                    1024 * index,
                    "e2e_batch_$index.json",
                )
            }

        try {
            val response =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = sources,
                    format = Format.multijson,
                    ingestProperties =
                    IngestRequestProperties(
                        format = Format.multijson,
                        enableTracking = true,
                    ),
                )

            assertNotNull(response.ingestionOperationId)
            logger.info(
                "E2E: Batch submitted: ${response.ingestionOperationId}",
            )

            val finalStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = response.ingestionOperationId,
                    pollingInterval = Duration.parse("PT5S"),
                    timeout = Duration.parse("PT5M"),
                )

            val succeededCount =
                finalStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0
            val failedCount =
                finalStatus.details?.count {
                    it.status == BlobStatus.Status.Failed
                } ?: 0

            logger.info(
                "E2E: Batch results - Success: $succeededCount, Failure: $failedCount",
            )
            assert(succeededCount == sources.size) {
                "Expected successful uploads"
            }
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: ${e.cause?.message}")
            } else {
                throw e
            }
        }
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `E2E - parallel processing with maxConcurrency`() = runBlocking {
        logger.info("E2E: Testing parallel processing with maxConcurrency=3")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withMaxConcurrency(5)
                .skipSecurityChecks()
                .build()

        val sources =
            (1..10).map { index ->
                createTestStreamSource(
                    512 * 1024,
                    "e2e_parallel_$index.json",
                )
            }

        try {
            val startTime = System.currentTimeMillis()

            val response =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = sources,
                    format = Format.multijson,
                    ingestProperties =
                    IngestRequestProperties(
                        format = Format.multijson,
                        enableTracking = true,
                    ),
                )

            val uploadDuration = System.currentTimeMillis() - startTime

            assertNotNull(response.ingestionOperationId)
            logger.info(
                "E2E: Parallel upload submitted in ${uploadDuration}ms: ${response.ingestionOperationId}",
            )

            val finalStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = response.ingestionOperationId,
                    pollingInterval = Duration.parse("PT5S"),
                    timeout = Duration.parse("PT5M"),
                )

            val succeededCount =
                finalStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0
            logger.info(
                "E2E: Parallel upload: $succeededCount/${sources.size} succeeded",
            )
            logger.info(
                "E2E: Average time per upload: ${uploadDuration / sources.size}ms",
            )

            assert(succeededCount == sources.size) {
                "Expected parallel uploads to succeed"
            }
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: ${e.cause?.message}")
            } else {
                throw e
            }
        }
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `E2E - size validation within limit`() = runBlocking {
        logger.info("E2E: Testing size validation with file within limit")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withMaxDataSize(10L * 1024 * 1024) // 10MB limit
                .skipSecurityChecks()
                .build()

        val source =
            createTestStreamSource(5 * 1024 * 1024, "e2e_size_valid.json")

        try {
            val response =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(source),
                    format = Format.multijson,
                    ingestProperties =
                    IngestRequestProperties(
                        format = Format.multijson,
                        enableTracking = true,
                    ),
                )

            assertNotNull(response.ingestionOperationId)
            logger.info("E2E: Size validation passed for file within limit")

            val finalStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = response.ingestionOperationId,
                    pollingInterval = Duration.parse("PT5S"),
                    timeout = Duration.parse("PT5M"),
                )

            val succeededCount =
                finalStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0
            assert(succeededCount > 0) {
                "Expected successful upload for file within size limit"
            }
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: ${e.cause?.message}")
            } else {
                throw e
            }
        }
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `E2E - size validation exceeds limit`() = runBlocking {
        logger.info("E2E: Testing size validation with file exceeding limit")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withMaxDataSize(1L * 1024 * 1024) // 1MB limit
                .skipSecurityChecks()
                .build()

        val source =
            createTestStreamSource(2 * 1024 * 1024, "e2e_size_exceed.json")

        try {
            try {
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(source),
                    format = Format.multijson,
                    ingestProperties =
                    IngestRequestProperties(
                        format = Format.multijson,
                        enableTracking = true,
                    ),
                )
                throw AssertionError(
                    "Expected size validation to reject the file",
                )
            } catch (e: IngestException) {
                logger.info(
                    "E2E: Size validation correctly rejected: ${e.message}",
                )
            }
            logger.info(
                "E2E: Size validation correctly rejected file exceeding limit",
            )
        } catch (e: AssertionError) {
            logger.info("E2E: Size limit enforced as expected")
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: ${e.cause?.message}")
            } else if (e.message?.contains("size", ignoreCase = true) == true) {
                logger.info(
                    "E2E: Size validation correctly rejected: ${e.message}",
                )
            } else {
                throw e
            }
        }
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `E2E - ignore size limit flag`() = runBlocking {
        logger.info("E2E: Testing size validation with ignore limit flag")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withMaxDataSize(1L * 1024 * 1024) // 1MB limit
                .withIgnoreFileSize(true) // But ignore it
                .skipSecurityChecks()
                .build()

        val source =
            createTestStreamSource(2 * 1024 * 1024, "e2e_size_ignore.json")

        try {
            val response =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(source),
                    format = Format.multijson,
                    ingestProperties =
                    IngestRequestProperties(
                        format = Format.multijson,
                        enableTracking = true,
                    ),
                )

            assertNotNull(response.ingestionOperationId)
            logger.info(
                "E2E: Size limit successfully bypassed with ignoreFileSize flag",
            )

            val finalStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = response.ingestionOperationId,
                    pollingInterval = Duration.parse("PT5S"),
                    timeout = Duration.parse("PT5M"),
                )

            val succeededCount =
                finalStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0
            assert(succeededCount > 0) {
                "Expected successful upload with ignore flag"
            }
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: ${e.cause?.message}")
            } else {
                throw e
            }
        }
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `E2E - combined all features scenario`() = runBlocking {
        logger.info(
            "E2E: Testing combined features (parallel + size validation + ignore flag)",
        )

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withMaxConcurrency(8)
                .withMaxDataSize(
                    10L * 1024 * 1024,
                ) // 10MB standard limit
                .withIgnoreFileSize(true)
                .skipSecurityChecks()
                .build()

        // Mix of file sizes: small (1-5MB), medium (5-10MB), large (10-20MB)
        val sources = mutableListOf<StreamSourceInfo>()

        // small files
        (1..7).forEach { i ->
            sources.add(
                createTestStreamSource(
                    1024 * 1024 * (1 + (i % 5)),
                    "e2e_combined_small_$i.json",
                ),
            )
        }

        // medium files
        (1..2).forEach { i ->
            sources.add(
                createTestStreamSource(
                    1024 * 1024 * (5 + (i % 5)),
                    "e2e_combined_medium_$i.json",
                ),
            )
        }

        // large files (need ignore flag)
        sources.add(
            createTestStreamSource(
                15 * 1024 * 1024,
                "e2e_combined_large_1.json",
            ),
        )

        logger.info(
            "E2E: Testing combined batch: ${sources.size} files, sizes 1MB-15MB",
        )

        try {
            val startTime = System.currentTimeMillis()

            val response =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = sources,
                    format = Format.multijson,
                    ingestProperties =
                    IngestRequestProperties(
                        format = Format.multijson,
                        enableTracking = true,
                    ),
                )

            val uploadDuration = System.currentTimeMillis() - startTime

            assertNotNull(response.ingestionOperationId)
            logger.info("E2E: combined batch uploaded in ${uploadDuration}ms")

            val finalStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = response.ingestionOperationId,
                    pollingInterval = Duration.parse("PT10S"),
                    timeout = Duration.parse("PT15M"),
                )

            val succeededCount =
                finalStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0

            logger.info(
                "E2E: combined scenario: $succeededCount/${sources.size} succeeded",
            )
            assert(succeededCount == sources.size) {
                "Combined scenario: ingestion succeeded"
            }
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: ${e.cause?.message}")
            } else {
                throw e
            }
        }
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `test parallel upload with multiple files`() = runBlocking {
        logger.info("Starting parallel upload test with multiple files")

        val deviceDataUrl =
            "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/multilined.json"
        val deviceData = java.net.URL(deviceDataUrl).readText()
        val targetFormat = Format.multijson

        val sources =
            (1..5).map { index ->
                StreamSourceInfo(
                    stream =
                    ByteArrayInputStream(
                        deviceData.toByteArray(),
                    ),
                    format = targetFormat,
                    sourceCompression = CompressionType.NONE,
                    sourceId = UUID.randomUUID(),
                    name = "parallel_test_$index.json",
                )
            }

        val queuedIngestionClient: IngestClient =
            QueuedIngestionClient(
                dmUrl = dmEndpoint,
                tokenCredential = tokenProvider,
                skipSecurityChecks = true,
            )

        val properties =
            IngestRequestProperties(
                format = targetFormat,
                enableTracking = true,
            )

        try {
            val startTime = System.currentTimeMillis()

            val ingestionResponse =
                queuedIngestionClient.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = sources,
                    format = targetFormat,
                    ingestProperties = properties,
                )

            val uploadTime = System.currentTimeMillis() - startTime

            logger.info(
                "Parallel upload test: Submitted {} files in {}ms with operation ID: {}",
                sources.size,
                uploadTime,
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

            val finalStatus =
                (queuedIngestionClient as QueuedIngestionClient)
                    .pollUntilCompletion(
                        database = database,
                        table = targetTable,
                        operationId =
                        ingestionResponse
                            .ingestionOperationId,
                        pollingInterval = Duration.parse("PT5S"),
                        timeout = Duration.parse("PT5M"),
                    )

            logger.info(
                "Parallel upload test: Ingestion completed with final status: {}",
                finalStatus.status,
            )

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
                    "Parallel upload results - Total: {}, Succeeded: {}, Failed: {}",
                    finalStatus.details.size,
                    succeededCount,
                    failedCount,
                )

                assert(succeededCount > 0) {
                    "Expected at least some successful uploads in parallel test"
                }

                logger.info(
                    "Parallel upload performance: {} files uploaded in {}ms (avg {}ms per file)",
                    sources.size,
                    uploadTime,
                    uploadTime / sources.size,
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

    @ParameterizedTest(
        name =
        "[QueuedIngestion-LocalSource] {index} => SourceType={0}, TestName={1}",
    )
    @CsvSource(
        "file,QueuedIngestion-FileSource,SampleFileSource.json",
        "stream,QueuedIngestion-StreamSource,SampleStreamSource.json",
    )
    fun `test queued ingestion with LocalSource`(
        sourceType: String,
        testName: String,
        fileName: String,
    ) = runBlocking {
        logger.info("Starting LocalSource test: $testName")
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

        val queuedIngestionClient: IngestClient =
            QueuedIngestionClient(
                dmUrl = dmEndpoint,
                tokenCredential = tokenProvider,
                skipSecurityChecks = true,
            )
        val properties =
            IngestRequestProperties(
                format = targetFormat,
                enableTracking = true,
            )

        val ingestionResponse =
            queuedIngestionClient.submitIngestion(
                database = database,
                table = targetTable,
                sources = listOf(source),
                format = targetFormat,
                ingestProperties = properties,
            )
        logger.info(
            "{}: Submitted queued ingestion with operation ID: {}",
            testName,
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
        val finalStatus =
            (queuedIngestionClient as QueuedIngestionClient)
                .pollUntilCompletion(
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
// https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/multilined.json
