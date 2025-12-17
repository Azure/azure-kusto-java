// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient
import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestClientException
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.ColumnMapping
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.InlineIngestionMapping
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.TransformationMethod
import com.microsoft.azure.kusto.ingest.v2.common.serialization.OffsetDateTimeSerializer
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo
import com.microsoft.azure.kusto.ingest.v2.models.ContainerSettings
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.azure.kusto.ingest.v2.source.FileSource
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.fail
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.io.ByteArrayInputStream
import java.net.ConnectException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.time.Clock
import java.time.Instant
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.test.assertNotNull
import kotlin.time.Duration

@Execution(ExecutionMode.CONCURRENT)
class QueuedIngestClientTest :
    IngestV2TestBase(QueuedIngestClientTest::class.java) {

    private val pollInterval = Duration.parse("PT2S")
    private val pollTimeout = Duration.parse("PT2M")

    private fun createTestClient(
        maxConcurrency: Int? = null,
        maxDataSize: Long? = null,
        ignoreFileSize: Boolean = false,
    ): QueuedIngestClient {
        val builder =
            QueuedIngestClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()

        maxConcurrency?.let { builder.withMaxConcurrency(it) }
        maxDataSize?.let { builder.withMaxDataSize(it) }
        if (ignoreFileSize) {
            builder.withIgnoreFileSize(true)
        }

        return builder.build()
    }

    private fun assertValidIngestionResponse(
        response: ExtendedIngestResponse,
        testName: String,
    ): String {
        assertNotNull(response, "$testName: IngestResponse should not be null")
        assertNotNull(
            response.ingestResponse.ingestionOperationId,
            "$testName: Operation ID should not be null",
        )
        return response.ingestResponse.ingestionOperationId
    }

    @Test
    fun `test builder variations`() {
        // builder with optional parameters
        val client1 = createTestClient(maxConcurrency = 10)
        assertNotNull(
            client1,
            "Client with optional parameters should not be null",
        )

        // builder with connector client details (uses custom configuration)
        val client2 =
            QueuedIngestClientBuilder.create(dmEndpoint)
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
                .skipSecurityChecks()
                .build()
        assertNotNull(
            client2,
            "Client with connector details should not be null",
        )

        // builder with connector client details and user (uses custom configuration)
        val client3 =
            QueuedIngestClientBuilder.create(dmEndpoint)
                .withAuthentication(tokenProvider)
                .withConnectorClientDetails(
                    name = "TestConnector",
                    version = "2.0",
                    sendUser = true,
                    overrideUser = "test-user@example.com",
                )
                .skipSecurityChecks()
                .build()
        assertNotNull(
            client3,
            "Client with connector details and user should not be null",
        )
    }

    @ParameterizedTest(name = "[QueuedIngestion] {index} => TestName ={0}")
    @CsvSource(
        // Single JSON blob, no mapping
        "QueuedIngestion-NoMapping,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json,false,false",
        // Single JSON blob, with mapping reference
        "QueuedIngestion-WithMappingReference,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json,true,false",
        // Single JSON blob, with inline mapping
        "QueuedIngestion-WithInlineMapping,https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json,false,true",
    )
    fun `test queued ingestion with blob variations`(
        testName: String,
        blobUrl: String,
        useMappingReference: Boolean,
        useInlineIngestionMapping: Boolean,
    ): Unit = runBlocking {
        logger.info("Starting test: $testName")
        val ingestClient = createTestClient()
        val testSources = listOf(BlobSource(blobUrl))

        val properties =
            if (useMappingReference) {
                IngestRequestPropertiesBuilder.create(database, targetTable)
                    .withFormat(targetTestFormat)
                    .withIngestionMappingReference(
                        "${targetTable}_mapping",
                    )
                    .withEnableTracking(true)
                    .build()
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
                    jsonPrinter.encodeToString(
                        inlineIngestionMappingInline.columnMappings,
                    )
                IngestRequestPropertiesBuilder.create(database, targetTable)
                    .withFormat(targetTestFormat)
                    .withIngestionMapping(ingestionMappingString)
                    .withEnableTracking(true)
                    .build()
            } else {
                IngestRequestPropertiesBuilder.create(database, targetTable)
                    .withFormat(targetTestFormat)
                    .withEnableTracking(true)
                    .build()
            }

        try {
            val ingestionResponse =
                ingestClient.ingestAsync(
                    sources = testSources,
                    ingestRequestProperties = properties,
                )

            val operationId =
                assertValidIngestionResponse(ingestionResponse, testName)
            val finalStatus =
                ingestClient.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = operationId,
                    pollingInterval = pollInterval,
                    timeout = pollTimeout,
                )

            logger.info(
                "{}: Polling completed with status:{}",
                testName,
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
                    "$testName: Succeeded: $succeededCount, Failed: $failedCount",
                )

                assert(succeededCount > 0 || failedCount > 0) {
                    "Expected at least some blobs to be processed"
                }
                assert(failedCount == 0) {
                    "Expected 0 failed ingestions, but got $failedCount"
                }

                if (failedCount > 0) {
                    finalStatus.details
                        .filter { it.status == BlobStatus.Status.Failed }
                        .forEach {
                            logger.error(
                                "Failed blob: ${it.sourceId}, message: ${it.details}",
                            )
                        }
                }

                val filterType =
                    when {
                        useMappingReference -> "MappingRef"
                        useInlineIngestionMapping -> "IngestionMapping"
                        else -> "None"
                    }
                if (useMappingReference || useInlineIngestionMapping) {
                    awaitAndQuery(
                        query =
                        "$targetTable | where Type == '$filterType' | summarize count=count() by SourceLocation",
                        expectedResultsCount = 5L,
                    )
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

    private fun createTestStreamSource(
        sizeInBytes: Int,
        name: String,
    ): StreamSource {
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

        return StreamSource(
            stream = ByteArrayInputStream(data),
            format = Format.multijson,
            sourceCompression = CompressionType.NONE,
            sourceId = UUID.randomUUID(),
            name = name,
        )
    }

    // definitely can be parallelized and optimized
    @Test
    fun `E2E - file size variations and batch uploads`() = runBlocking {
        logger.info("E2E: Testing combined file sizes (small, large, batch)")

        val queuedIngestClient = createTestClient()
        try {
            // Small file (1KB)
            logger.info("Testing small file upload (1KB)")
            val smallSource =
                createTestStreamSource(1024, "combined_small.json")
            val smallResponse =
                queuedIngestClient.ingestAsync(
                    source = smallSource,
                    ingestRequestProperties =
                    IngestRequestPropertiesBuilder.create(
                        database,
                        targetTable,
                    )
                        .withFormat(Format.multijson)
                        .withEnableTracking(true)
                        .build(),
                )
            assertNotNull(smallResponse.ingestResponse.ingestionOperationId)
            val smallStatus =
                queuedIngestClient.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId =
                    smallResponse.ingestResponse
                        .ingestionOperationId,
                    pollingInterval = pollInterval,
                    timeout = pollTimeout,
                )
            val smallSucceeded =
                smallStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0
            assert(smallSucceeded > 0) {
                "Expected successful small file ingestion"
            }
            logger.info(
                "Small file upload completed: $smallSucceeded succeeded",
            )

            // Large file (10MB)
            logger.info("Testing large file upload (10MB)")
            val largeSource =
                createTestStreamSource(
                    10 * 1024 * 1024,
                    "combined_large.json",
                )
            val largeResponse =
                queuedIngestClient.ingestAsync(
                    source = largeSource,
                    ingestRequestProperties =
                    IngestRequestPropertiesBuilder.create(
                        database,
                        targetTable,
                    )
                        .withFormat(Format.multijson)
                        .withEnableTracking(true)
                        .build(),
                )
            assertNotNull(largeResponse.ingestResponse.ingestionOperationId)
            val largeStatus =
                queuedIngestClient.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId =
                    largeResponse.ingestResponse
                        .ingestionOperationId,
                    pollingInterval = pollInterval,
                    timeout = pollTimeout,
                )
            val largeSucceeded =
                largeStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0
            assert(largeSucceeded > 0) {
                "Expected successful large file ingestion"
            }
            logger.info(
                "Large file upload completed: $largeSucceeded succeeded",
            )

            // Batch upload (5 files)
            logger.info("Testing batch upload (5 files)")
            val batchSources =
                (1..5).map { i ->
                    createTestStreamSource(
                        1024 * i,
                        "combined_batch_$i.json",
                    )
                }
            val batchResponse =
                queuedIngestClient.ingestAsync(
                    sources = batchSources,
                    ingestRequestProperties =
                    IngestRequestPropertiesBuilder.create(
                        database,
                        targetTable,
                    )
                        .withFormat(Format.multijson)
                        .withEnableTracking(true)
                        .build(),
                )
            assertNotNull(batchResponse.ingestResponse.ingestionOperationId)
            val batchStatus =
                queuedIngestClient.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId =
                    batchResponse.ingestResponse
                        .ingestionOperationId,
                    pollingInterval = pollInterval,
                    timeout = pollTimeout,
                )
            val batchSucceeded =
                batchStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0
            assert(batchSucceeded == batchSources.size) {
                "Expected all batch files to succeed"
            }
            logger.info(
                "Batch upload completed: $batchSucceeded/${batchSources.size} succeeded",
            )
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

        val queuedIngestClient = createTestClient(maxConcurrency = 5)

        val sources =
            (1..10).map { i ->
                createTestStreamSource(512 * 1024, "parallel_$i.json")
            }

        try {
            val startTime = System.currentTimeMillis()
            val response =
                queuedIngestClient.ingestAsync(
                    sources = sources,
                    ingestRequestProperties =
                    IngestRequestPropertiesBuilder.create(
                        database,
                        targetTable,
                    )
                        .withFormat(Format.multijson)
                        .withEnableTracking(true)
                        .build(),
                )
            val uploadDuration = System.currentTimeMillis() - startTime

            val operationId =
                assertValidIngestionResponse(
                    response,
                    "E2E - parallel processing",
                )
            logger.info(
                "Parallel upload submitted in ${uploadDuration}ms with operation ID: $operationId",
            )

            val finalStatus =
                queuedIngestClient.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId = operationId,
                    pollingInterval = pollInterval,
                    timeout = pollTimeout,
                )

            val succeededCount =
                finalStatus.details?.count {
                    it.status == BlobStatus.Status.Succeeded
                } ?: 0
            logger.info(
                "Parallel upload: $succeededCount/${sources.size} succeeded (avg ${uploadDuration / sources.size}ms per file)",
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

    private val jsonPrinter = Json {
        serializersModule = SerializersModule {
            contextual(OffsetDateTime::class, OffsetDateTimeSerializer)
        }
    }

    @ParameterizedTest(
        name =
        "[CompressionFormat] {index} => Format={0}, File={1}, Compression={2}",
    )
    @CsvSource(
        // Format, Resource file path, Compression type, Expected success
        "multijson,compression/sample.multijson,NONE,1",
        "multijson,compression/sample.multijson.gz,GZIP,1",
        "multijson,compression/sample.multijson.zip,ZIP,1",
        "json,compression/sample.json,NONE,3",
        "parquet,compression/sample.parquet,NONE,1",
        "avro,compression/sample.avro,NONE,1",
    )
    fun `E2E - compression format tests`(
        formatName: String,
        resourcePath: String,
        compressionTypeName: String,
        expectedRecordCount: Int,
    ): Unit = runBlocking {
        logger.info(
            "E2E: Testing format=$formatName, compression=$compressionTypeName, file=$resourcePath",
        )

        val queuedIngestClient = createTestClient()
        try {
            val resourceFile =
                this::class.java.classLoader.getResource(resourcePath)
            if (resourceFile == null) {
                logger.warn(
                    "Skipping test: Resource file not found: $resourcePath",
                )
                assumeTrue(false, "Resource file not found: $resourcePath")
                return@runBlocking
            }

            val format = Format.valueOf(formatName)
            val compressionType = CompressionType.valueOf(compressionTypeName)
            val fileExtension = resourcePath.substringAfterLast('.')

            val tempFile =
                Files.createTempFile("test_$formatName", ".$fileExtension")
            Files.copy(
                resourceFile.openStream(),
                tempFile,
                StandardCopyOption.REPLACE_EXISTING,
            )

            val source =
                FileSource(
                    path = tempFile,
                    format = format,
                    compressionType = compressionType,
                    sourceId = UUID.randomUUID(),
                )

            try {
                val createdTimeTag =
                    OffsetDateTime.now(Clock.systemUTC())
                        .minusHours((1..5L).random())
                val extentTags =
                    listOf("ingest-by:i-tag") + listOf("drop-by:d-tag")
                val response =
                    queuedIngestClient.ingestAsync(
                        sources = listOf(source),
                        ingestRequestProperties =
                        IngestRequestPropertiesBuilder.create(
                            database,
                            targetTable,
                        )
                            .withFormat(format)
                            .withEnableTracking(true)
                            .withIngestByTags(
                                listOf("i-tag"),
                            )
                            .withDropByTags(listOf("d-tag"))
                            .withCreationTime(
                                createdTimeTag,
                            )
                            .build(),
                    )

                val operationId =
                    assertValidIngestionResponse(
                        response,
                        "$formatName format test",
                    )
                logger.info(
                    "$formatName format test: submitted with operation ID $operationId",
                )

                val status =
                    queuedIngestClient.pollUntilCompletion(
                        database = database,
                        table = targetTable,
                        operationId = operationId,
                        pollingInterval = pollInterval,
                        timeout = pollTimeout,
                    )
                val succeededCount =
                    status.details?.count {
                        it.status == BlobStatus.Status.Succeeded
                    } ?: 0
                assert(succeededCount > 0) {
                    "Expected successful ingestion for $formatName and operation-id $operationId.Got response: " +
                        jsonPrinter.encodeToString(status)
                }
                logger.info(
                    "$formatName format test: passed ($succeededCount succeeded)",
                )
                awaitAndQuery(
                    query =
                    "$targetTable | where format == '$format' |summarize count=count() by format",
                    expectedResultsCount = expectedRecordCount.toLong(),
                )

                val extentDetailsResults =
                    adminClusterClient
                        .executeMgmt(
                            database,
                            ".show table $targetTable extents | project MinCreatedOn,Tags",
                        )
                        .primaryResults
                assertNotNull(
                    extentDetailsResults,
                    "Query results should not be null",
                )
                extentDetailsResults.next()
                val actualTags: String = extentDetailsResults.getString("Tags")
                /* TODO : This is being checked in the ingestion service side now. Uncomment when confirmed */
                val actualCreatedOnTime: Instant =
                    Instant.parse(
                        extentDetailsResults.getString("MinCreatedOn"),
                    )
                assertNotNull(
                    actualCreatedOnTime,
                    "Extent timestamp should not be null",
                )
                assertNotNull(actualTags, "Extent timestamp should not be null")

                val actualCreatedOnInstant =
                    actualCreatedOnTime.truncatedTo(ChronoUnit.SECONDS)
                val expectedCreatedOnInstant =
                    createdTimeTag
                        .toInstant()
                        .truncatedTo(ChronoUnit.SECONDS)
                assert(actualCreatedOnInstant == expectedCreatedOnInstant) {
                    "Extent creation time $actualCreatedOnInstant is <> expected $expectedCreatedOnInstant (rounded to seconds)"
                }
                extentTags.forEach { tag ->
                    assert(actualTags.contains(tag)) {
                        "Extent tags $actualTags does not contain expected tag $tag"
                    }
                }
            } catch (e: Exception) {
                e.printStackTrace()
                fail("Ingestion failed for $formatName: ${e.message}")
            } finally {
                Files.deleteIfExists(tempFile)
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
    fun `E2E - format mismatch and mixed format batch`(): Unit = runBlocking {
        logger.info("E2E: Testing format mismatch detection with mixed formats")

        val client = createTestClient()

        val jsonContent =
            """{"name":"test","value":123,"timestamp":"2024-01-01"}"""
        val csvContent =
            """name,value,timestamp
test,123,2024-01-01
test2,456,2024-01-02"""

        val sources =
            listOf(
                StreamSource(
                    stream =
                    ByteArrayInputStream(
                        jsonContent.toByteArray(),
                    ),
                    format = Format.json,
                    sourceCompression = CompressionType.NONE,
                    sourceId = UUID.randomUUID(),
                    name = "format_json.json",
                ),
                StreamSource(
                    stream =
                    ByteArrayInputStream(
                        csvContent.toByteArray(),
                    ),
                    format = Format.csv,
                    sourceCompression = CompressionType.NONE,
                    sourceId = UUID.randomUUID(),
                    name = "format_csv.csv",
                ),
                StreamSource(
                    stream =
                    ByteArrayInputStream(
                        jsonContent.toByteArray(),
                    ),
                    format = Format.json,
                    sourceCompression = CompressionType.NONE,
                    sourceId = UUID.randomUUID(),
                    name = "format_json2.json",
                ),
            )

        logger.info(
            "Uploading ${sources.size} sources with mixed formats (JSON and CSV)",
        )
        val exception =
            assertThrows<IngestClientException> {
                client.ingestAsync(
                    sources = sources,
                    ingestRequestProperties =
                    IngestRequestPropertiesBuilder.create(
                        database,
                        targetTable,
                    )
                        .withFormat(Format.json)
                        .withEnableTracking(true)
                        .build(),
                )
            }
        assertNotNull(
            exception,
            "Mixed formats are not permitted for ingestion",
        )
    }

    @ParameterizedTest(
        name = "[LocalSource] {index} => SourceType={0}, TestName={1}",
    )
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
        val deviceDataUrl =
            "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/multilined.json"
        val deviceData = java.net.URL(deviceDataUrl).readText()
        val targetFormat = Format.multijson
        val source: IngestionSource =
            when (sourceType) {
                "file" -> {
                    val tempFile = Files.createTempFile(fileName, null)
                    Files.write(tempFile, deviceData.toByteArray())
                    FileSource(
                        path = tempFile,
                        format = targetFormat,
                        compressionType = CompressionType.NONE,
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
                    StreamSource(
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

        val queuedIngestClient = createTestClient()
        val properties =
            IngestRequestPropertiesBuilder.create(database, targetTable)
                .withFormat(targetFormat)
                .withEnableTracking(true)
                .build()

        val ingestionResponse =
            queuedIngestClient.ingestAsync(
                sources = listOf(source),
                ingestRequestProperties = properties,
            )

        val operationId =
            assertValidIngestionResponse(ingestionResponse, testName)
        logger.info("$testName: Submitted with operation ID: $operationId")

        val finalStatus =
            queuedIngestClient.pollUntilCompletion(
                database = database,
                table = targetTable,
                operationId = operationId,
                pollingInterval = pollInterval,
                timeout = pollTimeout,
            )
        logger.info("$testName: Completed with status: ${finalStatus.status}")
        assert(
            finalStatus.details?.any {
                it.status == BlobStatus.Status.Succeeded
            } == true,
        ) {
            "Expected at least one successful ingestion for $testName"
        }
    }

    @Test
    fun `E2E - OneLake uploader test`(): Unit = runBlocking {
        if (oneLakeFolder != null) {
            logger.info("E2E: Testing OneLake uploader")

            // Create a ConfigurationResponse with OneLake lakeFolders configuration
            val oneLakeConfigResponse =
                ConfigurationResponse(
                    containerSettings =
                    ContainerSettings(
                        containers = null,
                        lakeFolders =
                        listOf(
                            ContainerInfo(
                                path =
                                oneLakeFolder,
                            ),
                        ),
                        refreshInterval = null,
                        preferredUploadMethod = "Rest",
                    ),
                    ingestionSettings = null,
                )

            // Create a configuration cache that returns our OneLake configuration
            val oneLakeConfiguration =
                DefaultConfigurationCache(
                    dmUrl = dmEndpoint,
                    tokenCredential = tokenProvider,
                    skipSecurityChecks = true,
                    clientDetails = ClientDetails.createDefault(),
                    configurationProvider = { oneLakeConfigResponse },
                )

            val builder =
                QueuedIngestClientBuilder.create(dmEndpoint)
                    .withAuthentication(tokenProvider)
                    .withConfiguration(oneLakeConfiguration)
                    .skipSecurityChecks()

            val oneLakeIngestClient = builder.build()

            val source = createTestStreamSource(1024 * 10, "onelake_test.json")

            try {
                val response =
                    oneLakeIngestClient.ingestAsync(
                        source = source,
                        ingestRequestProperties =
                        IngestRequestPropertiesBuilder.create(
                            database,
                            targetTable,
                        )
                            .withFormat(Format.multijson)
                            .withEnableTracking(true)
                            .build(),
                    )

                val operationId =
                    assertValidIngestionResponse(
                        response,
                        "OneLake uploader test",
                    )
                logger.info(
                    "OneLake uploader test: submitted with operation ID $operationId",
                )
            } catch (e: Exception) {
                e.printStackTrace()
                fail("Ingestion failed for OneLake uploader: ${e.message}")
            }
        } else {
            logger.warn(
                "Skipping OneLake uploader test: ONE_LAKE_FOLDER not set",
            )
            assumeTrue(false, "ONE_LAKE_FOLDER environment variable is not set")
        }
    }
}
