// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.builders.StreamingIngestClientBuilder
import com.microsoft.azure.kusto.ingest.v2.client.IngestClient
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.azure.kusto.ingest.v2.source.FileSource
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource
import kotlinx.coroutines.runBlocking
import java.io.ByteArrayInputStream
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import java.nio.file.Paths
import java.util.UUID
import java.util.stream.Stream
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StreamingIngestClientTest :
    IngestV2TestBase(StreamingIngestClientTest::class.java) {

    override fun additionalSetup() {
        // Enable streaming ingestion policy for all streaming tests
        alterTableToEnableStreaming()
        clearDatabaseSchemaCache()
    }

    private val publicBlobUrl =
        "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json"
    private fun testParameters(): Stream<Arguments?> {
        return Stream.of(
            Arguments.of(
                "Direct ingest - success",
                engineEndpoint,
                // isException
                // isUnreachableHost
                false,
                // isUnreachableHost
                false,
                // blobUrl
                null,
            ),
            Arguments.of(
                "Blob based ingest - success",
                engineEndpoint,
                // isException
                false,
                // isUnreachableHost
                false,
                publicBlobUrl,
            ),
            Arguments.of(
                "Blob based ingest- Invalid blob URL",
                engineEndpoint,
                // isException
                true,
                // isUnreachableHost
                false,
                "https://nonexistentaccount.blob.core.windows.net/container/file.json",
            ),
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testParameters")
    fun `run streaming ingest test using builder pattern`(
        testName: String,
        cluster: String,
        isException: Boolean,
        isUnreachableHost: Boolean,
        blobUrl: String?,
    ) = runBlocking {
        logger.info("Running streaming ingest builder test {}", testName)
        
        // Create client using builder
        val client: IngestClient =
            StreamingIngestClientBuilder.create(cluster)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .withClientDetails("BuilderStreamingE2ETest", "1.0")
                .build()

        val ingestProps =
            IngestRequestPropertiesBuilder.create(database, targetTable)
                .build()
        if (isException) {
            if (blobUrl != null) {
                logger.info(
                    "Testing error handling for invalid blob URL with builder: {} for unreachable host: {}",
                    blobUrl,
                    isUnreachableHost,
                )
                val exception =
                    assertThrows<IngestException> {
                        val ingestionSource =
                            BlobSource(blobUrl, format = Format.json)
                        client.ingestAsync(
                            source = ingestionSource,
                            ingestRequestProperties = ingestProps,
                        )
                    }
                assertNotNull(
                    exception,
                    "Exception should not be null for invalid blob URL",
                )
                logger.info(
                    "Expected exception caught (builder test): {}",
                    exception.message,
                )
                assert(exception.failureCode != 0) {
                    "Expected non-zero failure code for invalid blob URL"
                }
            }
        } else {
            if (blobUrl != null) {
                val ingestionSource = BlobSource(blobUrl, format = Format.json)
                client.ingestAsync(
                    source = ingestionSource,
                    ingestRequestProperties = ingestProps,
                )

                logger.info(
                    "Blob-based streaming ingestion submitted successfully (builder)",
                )
                awaitAndQuery(
                    query = "$targetTable | summarize count=count()",
                    expectedResultsCount = 5,
                )
            }
        }
    }

    /**
     * Test that error response parsing correctly extracts Kusto error details
     * from OneApiError JSON format.
     *
     * This validates that when streaming ingestion fails, the error message
     * contains meaningful details from the Kusto error response (code, type,
     * @message) rather than just a generic HTTP status code.
     */
    @org.junit.jupiter.api.Test
    fun `error response parsing extracts Kusto OneApiError details`() = runBlocking {
        logger.info("Testing error parsing for invalid data format")

        val client: IngestClient =
            StreamingIngestClientBuilder.create(engineEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .withClientDetails("ErrorParsingE2ETest", "1.0")
                .build()

        val properties =
            IngestRequestPropertiesBuilder.create(database, targetTable).build()

        // Send invalid text data claiming to be JSON - this triggers a data format error
        val invalidData = "this is not valid json { broken"
        val streamSource = StreamSource(
            stream = ByteArrayInputStream(invalidData.toByteArray()),
            format = Format.json,
            sourceId = UUID.randomUUID(),
            sourceCompression = CompressionType.NONE,
        )

        val exception = assertThrows<IngestException> {
            client.ingestAsync(
                source = streamSource,
                ingestRequestProperties = properties,
            )
        }

        // Validate exception was captured
        assertNotNull(exception, "Exception should not be null for invalid data format")

        // Log exception details for debugging
        logger.info("  Exception type: ${exception::class.simpleName}")
        logger.info("  Message: ${exception.message}")
        logger.info("  isPermanent: ${exception.isPermanent}")
        logger.info("  failureCode: ${exception.failureCode}")

        // Validate error parsing extracted meaningful details from Kusto OneApiError
        assert(exception.message.isNotEmpty()) {
            "Exception message should contain error details from Kusto OneApiError"
        }

        // Data format errors should be marked as permanent (cannot be retried)
        assert(exception.isPermanent == true) {
            "Data format errors should be marked as permanent"
        }
    }

    @ParameterizedTest(name = "Ingest file: {0} with compression: {1}")
    @CsvSource("sample.multijson,NONE", "sample.multijson.gz,GZIP")
    fun `ingest from file in compressed and uncompressed formats`(
        fileName: String,
        compressionType: String,
    ) = runBlocking {
        logger.info(
            "Running streaming ingest from file test: $fileName with compression: $compressionType",
        )

        // Create client using builder
        val client: IngestClient =
            StreamingIngestClientBuilder.create(engineEndpoint)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .withClientDetails("FileStreamingE2ETest", "1.0")
                .build()

        val resourcesDirectory = "src/test/resources/compression/"

        val compression =
            when (compressionType) {
                "NONE" -> CompressionType.NONE
                "GZIP" -> CompressionType.GZIP
                "ZIP" -> CompressionType.ZIP
                else ->
                    throw IllegalArgumentException(
                        "Unknown compression type: $compressionType",
                    )
            }

        val fileSource =
            FileSource(
                path = Paths.get(resourcesDirectory + fileName),
                format = Format.multijson,
                sourceId = UUID.randomUUID(),
                compressionType = compression,
            )

        val properties =
            IngestRequestPropertiesBuilder.create(database, targetTable)
                .withEnableTracking(true)
                .build()

        val response =
            client.ingestAsync(
                source = fileSource,
                ingestRequestProperties = properties,
            )

        assertNotNull(
            response,
            "File ingestion response should not be null for $fileName",
        )
        logger.info(
            "File ingestion completed for $fileName ($compressionType). Operation ID: ${response.ingestResponse.ingestionOperationId}",
        )
    }
}
