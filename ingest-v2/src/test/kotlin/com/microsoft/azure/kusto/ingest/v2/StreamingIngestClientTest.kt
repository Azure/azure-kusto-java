// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
class StreamingIngestClientTest :
    IngestV2TestBase(StreamingIngestClientTest::class.java) {

    private val publicBlobUrl = "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json"

    private val targetUuid = UUID.randomUUID().toString()
    private val randomRow: String =
        """{"timestamp": "2023-05-02 15:23:50.0000000","deviceId": "$targetUuid","messageId": "7f316225-839a-4593-92b5-1812949279b3","temperature": 31.0301639051317,"humidity": 62.0791099602725}"""
            .trimIndent()

    private fun testParameters(): Stream<Arguments?> {
        return Stream.of(
            //            Arguments.of(
            //                "Cluster without streaming ingest",
            //                "https://help.kusto.windows.net",
            //                true,
            //                false,
            //            ),
            // Blob-based streaming - error case
            Arguments.of(
                "Streaming ingest - Regular flow",
                engineEndpoint,
                false,
                // isUnreachableHost
                false,
                null, // blobUrl
            ),
        )
    }

    private fun blobStreamingTestParameters(): Stream<Arguments?> {
        return Stream.of(
            Arguments.of(
                "Direct streaming - backward compatibility",
                engineEndpoint,
                null, // passing blobUrl = null for direct streaming
            ),
            Arguments.of(
                "Blob-based streaming - E2E with public blob",
                engineEndpoint,
                publicBlobUrl,
            ),
        )
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
        val client = StreamingIngestClient(cluster, tokenProvider, true)
        val ingestProps = IngestRequestProperties(format = targetTestFormat)
        if (isException) {
            val table = "testtable"
            val data = "col1,col2\nval1,val2".toByteArray()
            val exception =
                assertThrows<IngestException> {
                    client.submitStreamingIngestion(
                        database,
                        table,
                        data,
                        targetTestFormat,
                        ingestProps,
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
        } else {
            // Perform streaming ingestion
            client.submitStreamingIngestion(
                database = database,
                table = targetTable,
                data = randomRow.toByteArray(),
                format = targetTestFormat,
                ingestProperties = ingestProps,
                blobUrl = blobUrl,
            )
            // Query the ingested data
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("blobStreamingTestParameters")
    fun `run blob streaming ingest test with various modes`(
        testName: String,
        cluster: String,
        blobUrl: String?,
    ) = runBlocking {
        logger.info("Running blob streaming ingest test: {}", testName)
        val client = StreamingIngestClient(cluster, tokenProvider, true)
        val ingestProps = IngestRequestProperties(format = targetTestFormat)

        val testUuid = UUID.randomUUID().toString()
        val testRow = """{"timestamp": "2023-05-02 15:23:50.0000000","deviceId": "$testUuid","messageId": "blob-test-message","temperature": 27.5,"humidity": 58.0}"""

        if (blobUrl != null) {
            logger.info("Blob-based streaming ingestion with URL: {}", blobUrl)

            client.submitStreamingIngestion(
                database = database,
                table = targetTable,
                data = ByteArray(0), // Ignored when blobUrl is provided
                format = targetTestFormat,
                ingestProperties = ingestProps,
                blobUrl = blobUrl,
            )

            logger.info("Blob-based streaming ingestion submitted successfully")

            kotlinx.coroutines.delay(3000)

            val results = adminClusterClient
                .executeQuery(
                    database,
                    "$targetTable | summarize count=count()"
                )
                .primaryResults

            assertNotNull(results, "Query results should not be null")
            results.next()
            val count: Long = results.getLong("count")
            assertNotNull(count, "Count should not be null")
            assert(count > 0) {
                "Expected records in table after blob-based streaming ingestion, but got $count"
            }

            logger.info("Blob-based streaming ingestion verified - {} records in table", count)
        } else {
            // Direct streaming ingestion (backward compatibility test)
            logger.info("Testing direct streaming")
            client.submitStreamingIngestion(
                database = database,
                table = targetTable,
                data = testRow.toByteArray(),
                format = targetTestFormat,
                ingestProperties = ingestProps,
                blobUrl = null,
            )

            // Query the ingested data
            val results = adminClusterClient
                .executeQuery(
                    database,
                    "$targetTable | where deviceId == '$testUuid' | summarize count=count() by deviceId"
                )
                .primaryResults

            assertNotNull(results, "Query results should not be null")
            results.next()
            val count: Long = results.getLong("count")
            assertNotNull(count, "Count should not be null")
            assert(count == 1L) {
                "Expected 1 record for $testUuid in test '$testName', but got $count"
            }
        }

        logger.info("Blob streaming test '{}' completed successfully", testName)
    }
}
