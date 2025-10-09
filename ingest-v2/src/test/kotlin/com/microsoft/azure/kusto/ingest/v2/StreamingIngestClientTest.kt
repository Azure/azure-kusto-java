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
            Arguments.of(
                "Streaming ingest - Regular flow",
                engineEndpoint,
                false,
                false,
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
                database,
                targetTable,
                randomRow.toByteArray(),
                targetTestFormat,
                ingestProps,
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
}
