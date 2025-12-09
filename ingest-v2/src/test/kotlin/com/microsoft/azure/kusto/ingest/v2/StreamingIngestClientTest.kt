// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.builders.StreamingIngestClientBuilder
import com.microsoft.azure.kusto.ingest.v2.client.IngestClient
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StreamingIngestClientTest :
    IngestV2TestBase(StreamingIngestClientTest::class.java) {

    private val publicBlobUrl =
        "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/simple.json"
    private val targetUuid = UUID.randomUUID().toString()

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

        val ingestProps = IngestRequestProperties(format = targetTestFormat)
        if (isException) {
            if (blobUrl != null) {
                logger.info(
                    "Testing error handling for invalid blob URL with builder: {} for unreachable host: {}",
                    blobUrl,
                    isUnreachableHost,
                )
                val exception =
                    assertThrows<IngestException> {
                        val ingestionSource = BlobSource(blobUrl)
                        client.ingestAsync(
                            database = database,
                            table = targetTable,
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
                val ingestionSource = BlobSource(blobUrl)
                client.ingestAsync(
                    database = database,
                    table = targetTable,
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
}
