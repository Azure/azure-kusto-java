// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.azure.kusto.ingest.v2.source.StreamSourceInfo
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.ByteArrayInputStream
import java.net.ConnectException
import java.util.UUID
import java.util.stream.Stream
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
class StreamingIngestClientTest :
    IngestV2TestBase(StreamingIngestClientTest::class.java) {

    private val publicBlobUrl =
        "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json"

    private val targetUuid = UUID.randomUUID().toString()
    private val randomRow: String =
        """{"timestamp": "2023-05-02 15:23:50.0000000","deviceId": "$targetUuid","messageId": "7f316225-839a-4593-92b5-1812949279b3","temperature": 31.0301639051317,"humidity": 62.0791099602725}"""
            .trimIndent()

    private fun testParameters(): Stream<Arguments?> {
        return Stream.of(
            Arguments.of(
                "Direct ingest - success",
                engineEndpoint,
                // isException
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
            com.microsoft.azure.kusto.ingest.v2.builders
                .StreamingIngestClientBuilder
                .create(cluster)
                .withAuthentication(tokenProvider)
                .skipSecurityChecks()
                .withClientDetails("BuilderStreamingE2ETest", "1.0")
                .build()

        val ingestProps = IngestRequestProperties(format = targetTestFormat)
        if (isException) {
            if (blobUrl != null) {
                logger.info(
                    "Testing error handling for invalid blob URL with builder: {}",
                    blobUrl,
                )
                val exception =
                    assertThrows<IngestException> {
                        val sources = listOf(BlobSourceInfo(blobUrl))
                        client.submitIngestion(
                            database = database,
                            table = targetTable,
                            sources = sources,
                            format = targetTestFormat,
                            ingestProperties = ingestProps,
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
                logger.info(
                    "Blob-based streaming ingestion with builder: {}",
                    blobUrl,
                )

                val sources = listOf(BlobSourceInfo(blobUrl))
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = sources,
                    format = targetTestFormat,
                    ingestProperties = ingestProps,
                )

                logger.info(
                    "Blob-based streaming ingestion submitted successfully (builder)",
                )

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
                    "Expected records in table after builder streaming ingestion"
                }

                logger.info(
                    "Builder streaming ingestion verified - {} records",
                    count,
                )
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testParameters")
    fun `run streaming ingest test with various clusters`(
        testName: String,
        cluster: String,
        isException: Boolean,
        isUnreachableHost: Boolean,
        blobUrl: String?,
    ) = runBlocking {
        logger.info("Running streaming ingest test {}", testName)
        val client: IngestClient =
            StreamingIngestClient(cluster, tokenProvider, true)
        val ingestProps = IngestRequestProperties(format = targetTestFormat)
        if (isException) {
            if (blobUrl != null) {
                logger.info(
                    "Testing error handling for invalid blob URL: {} (using interface method)",
                    blobUrl,
                )
                val exception =
                    assertThrows<IngestException> {
                        val sources = listOf(BlobSourceInfo(blobUrl))
                        client.submitIngestion(
                            database = database,
                            table = targetTable,
                            sources = sources,
                            format = targetTestFormat,
                            ingestProperties = ingestProps,
                        )
                    }
                assertNotNull(
                    exception,
                    "Exception should not be null for invalid blob URL",
                )
                logger.info(
                    "Expected exception caught for invalid blob URL: {}",
                    exception.message,
                )
                logger.info(
                    "Failure code: {}, isPermanent: {}",
                    exception.failureCode,
                    exception.isPermanent,
                )

                assert(exception.failureCode != 0) {
                    "Expected non-zero failure code for invalid blob URL"
                }
            } else {
                logger.info(
                    "Testing error handling for direct streaming ingestion",
                )
                val table = "testtable"
                val data = "col1,col2\nval1,val2".toByteArray()
                val exception =
                    assertThrows<IngestException> {
                        val streamSource =
                            StreamSourceInfo(
                                stream = ByteArrayInputStream(data),
                                format = targetTestFormat,
                                sourceCompression =
                                CompressionType.NONE,
                                sourceId = UUID.randomUUID(),
                                name = "error-test-stream",
                            )
                        client.submitIngestion(
                            database = database,
                            table = table,
                            sources = listOf(streamSource),
                            format = targetTestFormat,
                            ingestProperties = ingestProps,
                        )
                    }
                assertNotNull(exception, "Exception should not be null")
                if (isUnreachableHost) {
                    assert(exception.cause is java.net.ConnectException)
                    assert(exception.isPermanent == false)
                } else {
                    assert(exception.failureCode == 404)
                    assert(exception.isPermanent == false)
                }
            }
        } else {
            if (blobUrl != null) {
                logger.info(
                    "Blob-based streaming ingestion with URL: {}",
                    blobUrl,
                )

                val sources = listOf(BlobSourceInfo(blobUrl))
                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = sources,
                    format = targetTestFormat,
                    ingestProperties = ingestProps,
                )

                logger.info(
                    "Blob-based streaming ingestion submitted successfully",
                )

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
                    "Expected records in table after blob-based streaming ingestion, but got $count"
                }

                logger.info(
                    "Blob-based streaming ingestion verified - {} records in table",
                    count,
                )
            } else {
                logger.info("Direct streaming ingestion - success case")
                val streamSource =
                    StreamSourceInfo(
                        stream =
                        ByteArrayInputStream(
                            randomRow.toByteArray(),
                        ),
                        format = targetTestFormat,
                        sourceCompression = CompressionType.NONE,
                        sourceId = UUID.randomUUID(),
                        name = "direct-stream-$targetUuid",
                    )

                client.submitIngestion(
                    database = database,
                    table = targetTable,
                    sources = listOf(streamSource),
                    format = targetTestFormat,
                    ingestProperties = ingestProps,
                )

                val results =
                    adminClusterClient
                        .executeQuery(
                            database,
                            "$targetTable | where deviceId == '$targetUuid' | summarize count=count() by deviceId",
                        )
                        .primaryResults
                assertNotNull(results, "Query results should not be null")
                results.next()
                val count: Long = results.getLong("count")
                assertNotNull(count, "Count should not be null")
                assert(count == 1L) {
                    "Expected 1 record for $targetUuid, but got $count"
                }
            }
        }
    }
}
