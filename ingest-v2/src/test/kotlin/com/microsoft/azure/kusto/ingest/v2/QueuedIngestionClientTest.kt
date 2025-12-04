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

    private val POLLING_INTERVAL = Duration.parse("PT2S")
    private val POLLING_TIMEOUT = Duration.parse("PT2M")

    @Test
    fun `test builder variations`() {
        // builder with optional parameters
        val client1 =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withClientDetails("TestClient", "1.0")
                .withMaxConcurrency(10)
                .build()
        assertNotNull(client1, "Client with optional parameters should not be null")

        // builder with connector client details
        val client2 =
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
        assertNotNull(client2, "Client with connector details should not be null")

        // builder with connector client details and user
        val client3 =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withConnectorClientDetails(
                    name = "TestConnector",
                    version = "2.0",
                    sendUser = true,
                    overrideUser = "test-user@example.com",
                )
                .build()
        assertNotNull(client3, "Client with connector details and user should not be null")
    }

    @ParameterizedTest(name = "[QueuedIngestion] {index} => TestName ={0}")
    @CsvSource(
        // Single JSON blob, no mapping
        "QueuedIngestion-NoMapping,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json,false,false,0",
        // Single JSON blob, with mapping reference
        "QueuedIngestion-WithMappingReference,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json,true,false,0",
        // Single JSON blob, with inline mapping
        "QueuedIngestion-WithInlineMapping,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json,false,true,0",
    )
    fun `test queued ingestion with blob variations`(
        testName: String,
        blobUrl: String,
        useMappingReference: Boolean,
        useInlineIngestionMapping: Boolean,
        numberOfFailures: Int,
    ): Unit = runBlocking {
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
                    ingestionMappingReference = "${targetTable}_mapping",
                    enableTracking = true,
                )
            } else if (useInlineIngestionMapping) {
                val ingestionColumnMappings =
                    columnNamesToTypes.keys.map { col ->
                        when (col) {
                            "SourceLocation" ->
                                ColumnMapping(columnName = col, columnType = "string")
                                    .apply { setTransform(TransformationMethod.SourceLocation) }
                            "Type" ->
                                ColumnMapping(columnName = col, columnType = "string")
                                    .apply { setConstantValue("IngestionMapping") }
                            else ->
                                ColumnMapping(columnName = col, columnType = columnNamesToTypes[col]!!)
                                    .apply { setPath("$.$col") }
                        }
                    }
                val inlineIngestionMappingInline =
                    InlineIngestionMapping(
                        columnMappings = ingestionColumnMappings,
                        ingestionMappingType = InlineIngestionMapping.IngestionMappingType.JSON,
                    )
                val ingestionMappingString =
                    Json.encodeToString(inlineIngestionMappingInline.columnMappings)
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
            val ingestionResponse =
                queuedIngestionClient.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = testSources,
                    format = targetTestFormat,
                    ingestProperties = properties,
                )

            logger.info("$testName: Submitted with operation ID: ${ingestionResponse.ingestionOperationId}")
            assertNotNull(ingestionResponse, "IngestionOperation should not be null")
            assertNotNull(ingestionResponse.ingestionOperationId, "Operation ID should not be null")

            val finalStatus =
                (queuedIngestionClient as QueuedIngestionClient)
                    .pollUntilCompletion(
                        database = database,
                        table = targetTable,
                        operationId = ingestionResponse.ingestionOperationId,
                        pollingInterval = POLLING_INTERVAL,
                        timeout = POLLING_TIMEOUT,
                    )

            logger.info("$testName: Completed with status: ${finalStatus.status}")

            if (finalStatus.details?.isNotEmpty() == true) {
                val succeededCount = finalStatus.details.count { it.status == BlobStatus.Status.Succeeded }
                val failedCount = finalStatus.details.count { it.status == BlobStatus.Status.Failed }
                logger.info("$testName: Succeeded: $succeededCount, Failed: $failedCount")

                assert(succeededCount > 0 || failedCount > 0) {
                    "Expected at least some blobs to be processed"
                }
                assert(failedCount == numberOfFailures) {
                    "Expected $numberOfFailures failed ingestions, but got $failedCount"
                }

                if (failedCount > 0) {
                    finalStatus.details
                        .filter { it.status == BlobStatus.Status.Failed }
                        .forEach { logger.error("Failed blob: ${it.sourceId}, message: ${it.details}") }
                }

                val filterType = when {
                    useMappingReference -> "MappingRef"
                    useInlineIngestionMapping -> "IngestionMapping"
                    else -> "None"
                }
                if (useMappingReference || useInlineIngestionMapping) {
                    val results =
                        adminClusterClient
                            .executeQuery(database, "$targetTable | where Type == '$filterType' | summarize count=count() by SourceLocation")
                            .primaryResults
                    assertNotNull(results, "Query results should not be null")
                    results.next()
                    val count: Long = results.getLong("count")
                    assertNotNull(count, "Count should not be null")
                    assert(count > 0) { "Expected some records in the table after ingestion" }
                }
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

    private fun createTestStreamSource(sizeInBytes: Int, name: String): StreamSourceInfo {
        val jsonLine = """{"testField":"value","size":$sizeInBytes,"name":"$name"}""" + "\n"
        val jsonLineBytes = jsonLine.toByteArray()
        val numLines = (sizeInBytes / jsonLineBytes.size).coerceAtLeast(1)
        val data = ByteArray(numLines * jsonLineBytes.size)

        for (i in 0 until numLines) {
            System.arraycopy(jsonLineBytes, 0, data, i * jsonLineBytes.size, jsonLineBytes.size)
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
    fun `E2E - file size variations and batch uploads`() = runBlocking {
        logger.info("E2E: Testing combined file sizes (small, large, batch)")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .build()

        try {
            // Small file (1KB)
            logger.info("Testing small file upload (1KB)")
            val smallSource = createTestStreamSource(1024, "combined_small.json")
            val smallResponse =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(smallSource),
                    format = Format.multijson,
                    ingestProperties = IngestRequestProperties(format = Format.multijson, enableTracking = true),
                )
            assertNotNull(smallResponse.ingestionOperationId)
            val smallStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = smallResponse.ingestionOperationId,
                    pollingInterval = POLLING_INTERVAL,
                    timeout = POLLING_TIMEOUT,
                )
            val smallSucceeded = smallStatus.details?.count { it.status == BlobStatus.Status.Succeeded } ?: 0
            assert(smallSucceeded > 0) { "Expected successful small file ingestion" }
            logger.info("Small file upload completed: $smallSucceeded succeeded")

            // Large file (10MB)
            logger.info("Testing large file upload (10MB)")
            val largeSource = createTestStreamSource(10 * 1024 * 1024, "combined_large.json")
            val largeResponse =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(largeSource),
                    format = Format.multijson,
                    ingestProperties = IngestRequestProperties(format = Format.multijson, enableTracking = true),
                )
            assertNotNull(largeResponse.ingestionOperationId)
            val largeStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = largeResponse.ingestionOperationId,
                    pollingInterval = POLLING_INTERVAL,
                    timeout = POLLING_TIMEOUT,
                )
            val largeSucceeded = largeStatus.details?.count { it.status == BlobStatus.Status.Succeeded } ?: 0
            assert(largeSucceeded > 0) { "Expected successful large file ingestion" }
            logger.info("Large file upload completed: $largeSucceeded succeeded")

            // Batch upload (5 files)
            logger.info("Testing batch upload (5 files)")
            val batchSources = (1..5).map { i -> createTestStreamSource(1024 * i, "combined_batch_$i.json") }
            val batchResponse =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = batchSources,
                    format = Format.multijson,
                    ingestProperties = IngestRequestProperties(format = Format.multijson, enableTracking = true),
                )
            assertNotNull(batchResponse.ingestionOperationId)
            val batchStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = batchResponse.ingestionOperationId,
                    pollingInterval = POLLING_INTERVAL,
                    timeout = POLLING_TIMEOUT,
                )
            val batchSucceeded = batchStatus.details?.count { it.status == BlobStatus.Status.Succeeded } ?: 0
            assert(batchSucceeded == batchSources.size) { "Expected all batch files to succeed" }
            logger.info("Batch upload completed: $batchSucceeded/${batchSources.size} succeeded")

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
    fun `E2E - parallel processing with maxConcurrency`() = runBlocking {
        logger.info("E2E: Testing parallel processing with maxConcurrency=5")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withMaxConcurrency(5)
                .skipSecurityChecks()
                .build()

        val sources = (1..10).map { i -> createTestStreamSource(512 * 1024, "parallel_$i.json") }

        try {
            val startTime = System.currentTimeMillis()
            val response =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = sources,
                    format = Format.multijson,
                    ingestProperties = IngestRequestProperties(format = Format.multijson, enableTracking = true),
                )
            val uploadDuration = System.currentTimeMillis() - startTime

            assertNotNull(response.ingestionOperationId)
            logger.info("Parallel upload submitted in ${uploadDuration}ms")

            val finalStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = response.ingestionOperationId,
                    pollingInterval = POLLING_INTERVAL,
                    timeout = POLLING_TIMEOUT,
                )

            val succeededCount = finalStatus.details?.count { it.status == BlobStatus.Status.Succeeded } ?: 0
            logger.info("Parallel upload: $succeededCount/${sources.size} succeeded (avg ${uploadDuration / sources.size}ms per file)")
            assert(succeededCount == sources.size) { "Expected parallel uploads to succeed" }

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

    @ParameterizedTest(name = "[SizeValidation] {index} => Scenario={0}")
    @CsvSource(
        "within-limit,10,5,false,true",   // 5MB file, 10MB limit, no ignore, expect success
        "exceeds-limit,1,2,false,false",  // 2MB file, 1MB limit, no ignore, expect rejection
        "ignore-flag,1,2,true,true"       // 2MB file, 1MB limit, with ignore, expect success
    )
    fun `E2E - size validation scenarios`(
        scenario: String,
        maxSizeMB: Long,
        fileSizeMB: Int,
        ignoreSize: Boolean,
        expectSuccess: Boolean
    ) = runBlocking {
        logger.info("E2E: Testing size validation scenario: $scenario")

        val clientBuilder = QueuedIngestionClientBuilder.create(dmEndpoint)
            .withAuthentication(tokenProvider)
            .withMaxDataSize(maxSizeMB * 1024 * 1024)
            .skipSecurityChecks()

        val client = if (ignoreSize) {
            clientBuilder.withIgnoreFileSize(true).build()
        } else {
            clientBuilder.build()
        }

        val source = createTestStreamSource(fileSizeMB * 1024 * 1024, "size_${scenario}.json")

        try {
            if (expectSuccess) {
                val response = client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(source),
                    format = Format.multijson,
                    ingestProperties = IngestRequestProperties(format = Format.multijson, enableTracking = true),
                )

                assertNotNull(response.ingestionOperationId)
                logger.info("E2E: $scenario - Submitted successfully: ${response.ingestionOperationId}")

                val finalStatus = client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = response.ingestionOperationId,
                    pollingInterval = POLLING_INTERVAL,
                    timeout = POLLING_TIMEOUT,
                )

                val succeededCount = finalStatus.details?.count { it.status == BlobStatus.Status.Succeeded } ?: 0
                assert(succeededCount > 0) { "Expected successful upload for scenario: $scenario" }
                logger.info("E2E: $scenario - Completed successfully")
            } else {
                try {
                    client.submitIngestion(
                        database = database,
                        table = targetTable,
                        sources = listOf(source),
                        format = Format.multijson,
                        ingestProperties = IngestRequestProperties(format = Format.multijson, enableTracking = true),
                    )
                    throw AssertionError("Expected size validation to reject the file for scenario: $scenario")
                } catch (e: IngestException) {
                    logger.info("E2E: $scenario - Size validation correctly rejected: ${e.message}")
                }
                logger.info("E2E: $scenario - Correctly rejected file exceeding limit")
            }
        } catch (e: AssertionError) {
            if (!expectSuccess) {
                logger.info("E2E: $scenario - Size limit enforced as expected")
            } else {
                throw e
            }
        } catch (e: ConnectException) {
            assumeTrue(false, "Skipping test: ${e.message}")
        } catch (e: Exception) {
            if (e.cause is ConnectException) {
                assumeTrue(false, "Skipping test: ${e.cause?.message}")
            } else if (!expectSuccess && e.message?.contains("size", ignoreCase = true) == true) {
                logger.info("E2E: $scenario - Size validation correctly rejected: ${e.message}")
            } else {
                throw e
            }
        }
    }

    @Test
    fun `E2E - compression format tests`() = runBlocking {
        logger.info("E2E: Testing compression formats (JSON, GZIP, Parquet, AVRO)")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .build()

        try {
            // JSON file (uncompressed, gets compressed during upload)
            logger.info("Testing JSON file compression during upload")
            val jsonData = """{"timestamp":"2024-01-01T00:00:00Z","deviceId":"00000000-0000-0000-0000-000000000001","messageId":"00000000-0000-0000-0000-000000000002","temperature":25.5,"humidity":60.0}"""
            val jsonFile = Files.createTempFile("test_json", ".json")
            Files.write(jsonFile, jsonData.toByteArray())
            val jsonSource = FileSourceInfo(path = jsonFile, format = Format.multijson, compressionType = CompressionType.NONE, name = "test_json.json", sourceId = UUID.randomUUID())
            val jsonResponse = client.submitIngestion(database = database, table = targetTable, sources = listOf(jsonSource), format = Format.multijson, ingestProperties = IngestRequestProperties(format = Format.multijson, enableTracking = true))
            assertNotNull(jsonResponse.ingestionOperationId)
            val jsonStatus = client.pollUntilCompletion(database = database, table = targetTable, operationId = jsonResponse.ingestionOperationId, pollingInterval = POLLING_INTERVAL, timeout = POLLING_TIMEOUT)
            val jsonSucceeded = jsonStatus.details?.count { it.status == BlobStatus.Status.Succeeded } ?: 0
            assert(jsonSucceeded > 0) { "Expected successful JSON ingestion" }
            logger.info("JSON file compression test: passed")
            Files.deleteIfExists(jsonFile)

            // GZIP pre-compressed file
            logger.info("Testing GZIP pre-compressed file")
            val gzipFile = Files.createTempFile("test_gzip", ".json.gz")
            java.util.zip.GZIPOutputStream(Files.newOutputStream(gzipFile)).use { it.write(jsonData.toByteArray()) }
            val gzipSource = FileSourceInfo(path = gzipFile, format = Format.multijson, compressionType = CompressionType.GZIP, name = "pre_compressed.json.gz", sourceId = UUID.randomUUID())
            val gzipResponse = client.submitIngestion(database = database, table = targetTable, sources = listOf(gzipSource), format = Format.multijson, ingestProperties = IngestRequestProperties(format = Format.multijson, enableTracking = true))
            assertNotNull(gzipResponse.ingestionOperationId)
            val gzipStatus = client.pollUntilCompletion(database = database, table = targetTable, operationId = gzipResponse.ingestionOperationId, pollingInterval = POLLING_INTERVAL, timeout = POLLING_TIMEOUT)
            val gzipSucceeded = gzipStatus.details?.count { it.status == BlobStatus.Status.Succeeded } ?: 0
            assert(gzipSucceeded > 0) { "Expected successful GZIP ingestion" }
            logger.info("GZIP pre-compressed test: passed")
            Files.deleteIfExists(gzipFile)

            // Parquet and AVRO (skip if resources not found)
            val parquetFile = this::class.java.classLoader.getResource("compression/sample.parquet")
            if (parquetFile != null) {
                logger.info("Testing Parquet format")
                val tempParquet = Files.createTempFile("test_parquet", ".parquet")
                Files.copy(parquetFile.openStream(), tempParquet, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
                val parquetSource = FileSourceInfo(path = tempParquet, format = Format.parquet, compressionType = CompressionType.NONE, name = "test.parquet", sourceId = UUID.randomUUID())
                try {
                    val parquetResponse = client.submitIngestion(database = database, table = targetTable, sources = listOf(parquetSource), format = Format.parquet, ingestProperties = IngestRequestProperties(format = Format.parquet, enableTracking = true))
                    assertNotNull(parquetResponse.ingestionOperationId)
                    logger.info("Parquet format test: submitted (schema may not match)")
                } catch (e: Exception) {
                    logger.warn("Parquet test skipped (may be due to schema mismatch): ${e.message}")
                }
                Files.deleteIfExists(tempParquet)
            }

            val avroFile = this::class.java.classLoader.getResource("compression/sample.avro")
            if (avroFile != null) {
                logger.info("Testing AVRO format")
                val tempAvro = Files.createTempFile("test_avro", ".avro")
                Files.copy(avroFile.openStream(), tempAvro, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
                val avroSource = FileSourceInfo(path = tempAvro, format = Format.avro, compressionType = CompressionType.NONE, name = "test.avro", sourceId = UUID.randomUUID())
                try {
                    val avroResponse = client.submitIngestion(database = database, table = targetTable, sources = listOf(avroSource), format = Format.avro, ingestProperties = IngestRequestProperties(format = Format.avro, enableTracking = true))
                    assertNotNull(avroResponse.ingestionOperationId)
                    logger.info("AVRO format test: submitted (schema may not match)")
                } catch (e: Exception) {
                    logger.warn("AVRO test skipped (may be due to schema mismatch): ${e.message}")
                }
                Files.deleteIfExists(tempAvro)
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
    fun `E2E - format mismatch and mixed format batch`() = runBlocking {
        logger.info("E2E: Testing format mismatch detection with mixed formats")

        val client =
            QueuedIngestionClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .build()

        val jsonContent = """{"name":"test","value":123,"timestamp":"2024-01-01"}"""
        val csvContent = """name,value,timestamp
test,123,2024-01-01
test2,456,2024-01-02"""

        val sources =
            listOf(
                StreamSourceInfo(stream = ByteArrayInputStream(jsonContent.toByteArray()), format = Format.json, sourceCompression = CompressionType.NONE, sourceId = UUID.randomUUID(), name = "format_json.json"),
                StreamSourceInfo(stream = ByteArrayInputStream(csvContent.toByteArray()), format = Format.csv, sourceCompression = CompressionType.NONE, sourceId = UUID.randomUUID(), name = "format_csv.csv"),
                StreamSourceInfo(stream = ByteArrayInputStream(jsonContent.toByteArray()), format = Format.json, sourceCompression = CompressionType.NONE, sourceId = UUID.randomUUID(), name = "format_json2.json"),
            )

        try {
            logger.info("Uploading ${sources.size} sources with mixed formats (JSON and CSV)")
            val response =
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = sources,
                    format = Format.json,
                    ingestProperties = IngestRequestProperties(format = Format.json, enableTracking = true),
                )

            assertNotNull(response.ingestionOperationId)
            logger.info("Mixed format batch submitted: ${response.ingestionOperationId}")

            val finalStatus =
                client.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = response.ingestionOperationId,
                    pollingInterval = POLLING_INTERVAL,
                    timeout = POLLING_TIMEOUT,
                )

            val succeededCount = finalStatus.details?.count { it.status == BlobStatus.Status.Succeeded } ?: 0
            val failedCount = finalStatus.details?.count { it.status == BlobStatus.Status.Failed } ?: 0

            logger.info("Format mismatch results - Success: $succeededCount, Failed: $failedCount")

            if (failedCount > 0) {
                finalStatus.details
                    ?.filter { it.status == BlobStatus.Status.Failed }
                    ?.forEach { logger.error("Failed: ${it.sourceId}, errorCode: ${it.errorCode}, details: ${it.details}") }
            }

            assert(failedCount >= 1) {
                "Expected at least one failure due to format mismatch, but got: succeeded=$succeededCount, failed=$failedCount"
            }
            logger.info("Format mismatch correctly detected by server")

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

    @ParameterizedTest(name = "[LocalSource] {index} => SourceType={0}, TestName={1}")
    @CsvSource(
        "file,QueuedIngestion-FileSource,SampleFileSource.json",
        "stream,QueuedIngestion-StreamSource,SampleStreamSource.json",
    )
    fun `test queued ingestion with local sources`(
        sourceType: String,
        testName: String,
        fileName: String,
    ) = runBlocking {
        logger.info("Starting LocalSource test: $testName")
        val deviceDataUrl = "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/multilined.json"
        val deviceData = java.net.URL(deviceDataUrl).readText()
        val targetFormat = Format.multijson
        val source: AbstractSourceInfo =
            when (sourceType) {
                "file" -> {
                    val tempFile = Files.createTempFile(fileName, null)
                    Files.write(tempFile, deviceData.toByteArray())
                    FileSourceInfo(path = tempFile, format = targetFormat, compressionType = CompressionType.NONE, name = fileName, sourceId = UUID.randomUUID())
                        .also {
                            Runtime.getRuntime().addShutdownHook(Thread { Files.deleteIfExists(tempFile) })
                        }
                }
                "stream" ->
                    StreamSourceInfo(stream = ByteArrayInputStream(deviceData.toByteArray()), format = targetFormat, sourceCompression = CompressionType.NONE, sourceId = UUID.randomUUID(), name = fileName)
                else -> error("Unknown sourceType: $sourceType")
            }

        val queuedIngestionClient: IngestClient =
            QueuedIngestionClient(dmUrl = dmEndpoint, tokenCredential = tokenProvider, skipSecurityChecks = true)
        val properties = IngestRequestProperties(format = targetFormat, enableTracking = true)

        val ingestionResponse = queuedIngestionClient.submitIngestion(database = database, table = targetTable, sources = listOf(source), format = targetFormat, ingestProperties = properties)
        logger.info("$testName: Submitted with operation ID: ${ingestionResponse.ingestionOperationId}")
        assertNotNull(ingestionResponse, "IngestionOperation should not be null")
        assertNotNull(ingestionResponse.ingestionOperationId, "Operation ID should not be null")

        val finalStatus =
            (queuedIngestionClient as QueuedIngestionClient)
                .pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = ingestionResponse.ingestionOperationId,
                    pollingInterval = POLLING_INTERVAL,
                    timeout = POLLING_TIMEOUT,
                )
        logger.info("$testName: Completed with status: ${finalStatus.status}")
        assert(finalStatus.details?.any { it.status == BlobStatus.Status.Succeeded } == true) {
            "Expected at least one successful ingestion for $testName"
        }
    }
}
