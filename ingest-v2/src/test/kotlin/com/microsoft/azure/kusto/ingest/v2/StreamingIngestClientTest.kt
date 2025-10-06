// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
class StreamingIngestClientTest : IngestV2TestBase(StreamingIngestClientTest::class.java) {

    private fun endpointAndExceptionClause(): Stream<Arguments?> {
        return Stream.of(
            Arguments.of(
                "Unreachable cluster - Non existent host",
                "https://nonexistent.kusto.windows.net",
                true,
                true,
            ),
            Arguments.of(
                "Cluster without streaming ingest",
                "https://help.kusto.windows.net",
                true,
                false,
            ),
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("endpointAndExceptionClause")
    fun `run streaming ingest test with various clusters`(
        testName: String,
        cluster: String,
        isException: Boolean,
        isUnreachableHost: Boolean,
    ) = runBlocking {
        logger.info("Running streaming ingest test {}", testName)
        val client = StreamingIngestClient(cluster, tokenProvider, true)
        val database = "testdb"
        val table = "testtable"
        val data = "col1,col2\nval1,val2".toByteArray()
        val format = Format.csv
        val ingestProps =
            IngestRequestProperties(
                format = Format.csv,
                ignoreFirstRecord = true,
            )
        if (isException) {
            val exception =
                assertThrows<IngestException> {
                    client.submitStreamingIngestion(
                        database,
                        table,
                        data,
                        format,
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
        }
    }
}
