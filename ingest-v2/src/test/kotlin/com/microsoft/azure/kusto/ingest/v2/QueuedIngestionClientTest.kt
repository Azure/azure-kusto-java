// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestionClientBuilder
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
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
class QueuedIngestionClientTest :
    IngestV2TestBase(QueuedIngestionClientTest::class.java) {

    @Test
    fun `test builder with optional parameters`() {
        val client = QueuedIngestionClientBuilder
            .create(dmEndpoint)
            .withAuthentication(tokenProvider)
            .withClientDetails("TestClient", "1.0")
            .withMaxConcurrency(10)
            .build()
        
        assertNotNull(client, "Client should not be null")
    }

    @Test
    fun `test builder with connector client details`() {
        val client = QueuedIngestionClientBuilder
            .create(dmEndpoint)
            .withAuthentication(tokenProvider)
            .withConnectorClientDetails(
                name = "TestConnector",
                version = "2.0",
                appName = "MyApp",
                appVersion = "1.5",
                additionalFields = mapOf(
                    "JobId" to "job-123",
                    "RunId" to "run-456"
                )
            )
            .build()
        
        assertNotNull(client, "Client should not be null")
    }

    @Test
    fun `test builder with connector client details and user`() {
        val client = QueuedIngestionClientBuilder
            .create(dmEndpoint)
            .withAuthentication(tokenProvider)
            .withConnectorClientDetails(
                name = "TestConnector",
                version = "2.0",
                sendUser = true,
                overrideUser = "test-user@example.com"
            )
            .build()
        
        assertNotNull(client, "Client should not be null")
    }

    @Test
    @ResourceLock("blob-ingestion")
    fun `test queued ingestion with builder pattern`(): Unit = runBlocking {
        logger.info("Starting builder pattern test")

        val queuedIngestionClient: IngestClient = QueuedIngestionClientBuilder
            .create(dmEndpoint)
            .withAuthentication(tokenProvider)
            .skipSecurityChecks()
            .build()

        val blobUrl = "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json"
        val testSources = listOf(BlobSourceInfo(blobUrl))
        val properties = IngestRequestProperties(
            format = targetTestFormat,
            ingestionMappingReference = "${targetTable}_mapping",
            enableTracking = true,
        )

        try {
            val ingestionResponse = queuedIngestionClient.submitIngestion(
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
            assertNotNull(ingestionResponse, "IngestionOperation should not be null")
            assertNotNull(ingestionResponse.ingestionOperationId, "Operation ID should not be null")

            val finalStatus = (queuedIngestionClient as QueuedIngestionClient)
                .pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = ingestionResponse.ingestionOperationId,
                    pollingInterval = Duration.parse("PT5S"),
                    timeout = Duration.parse("PT5M"),
                )

            logger.info(
                "Builder pattern test: Ingestion completed with final status: {}",
                finalStatus.status,
            )

            if (finalStatus.details?.isNotEmpty() == true) {
                val succeededCount = finalStatus.details.count { it.status == BlobStatus.Status.Succeeded }
                val failedCount = finalStatus.details.count { it.status == BlobStatus.Status.Failed }
                logger.info("Builder pattern test: Succeeded: {}, Failed: {}", succeededCount, failedCount)

                assert(succeededCount > 0 || failedCount > 0) {
                    "Expected at least some blobs to be processed"
                }
            } else {
                logger.info("Builder pattern test: No details available, but operation was submitted successfully")
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

    @Test
    @ResourceLock("blob-ingestion")
    fun `test parallel upload with multiple files`() = runBlocking {
        logger.info("Starting parallel upload test with multiple files")
        
        val deviceDataUrl =
            "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/multilined.json"
        val deviceData = java.net.URL(deviceDataUrl).readText()
        val targetFormat = Format.multijson
        
        val sources = (1..5).map { index ->
            StreamSourceInfo(
                stream = ByteArrayInputStream(deviceData.toByteArray()),
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
            
            assertNotNull(ingestionResponse, "IngestionOperation should not be null")
            assertNotNull(ingestionResponse.ingestionOperationId, "Operation ID should not be null")
            
            val finalStatus =
                (queuedIngestionClient as QueuedIngestionClient)
                    .pollUntilCompletion(
                        database = database,
                        table = targetTable,
                        operationId = ingestionResponse.ingestionOperationId,
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
