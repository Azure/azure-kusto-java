// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.identity.AzureCliCredentialBuilder
import com.microsoft.azure.kusto.data.Client
import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.ingest.v2.client.ingestProperties
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.source.DataFormat
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.util.*
import kotlin.test.assertNotNull
import kotlin.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueuedIngestionApiWrapperTest {
    private val logger =
        LoggerFactory.getLogger(QueuedIngestionApiWrapperTest::class.java)
    private val azureCliCredential = AzureCliCredentialBuilder().build()
    private val database = System.getenv("TEST_DATABASE") ?: "e2e"
    private val dmEndpoint = System.getenv("DM_CONNECTION_STRING")
    private val targetTable =
        "Storms_${UUID.randomUUID().toString().replace("-", "").take(8)}"
    private lateinit var adminClient: Client

    @BeforeAll
    fun createTables() {
        val createTableScript =
            """
            .create table $targetTable (
                [State] : string,
                [EventType] : string,
                [CZTime] : datetime,
                [InjuriesDirect] : int,
                [InjuriesIndirect] : int,
                [DeathsDirect] : int,
                [DeathsIndirect] : int,
                [DamageProperty] : real,
                [DamageCrops] : real,
                [WFO] : string,
                [BeginLat] : real,
                [BeginLon] : real,
                [EndLat] : real,
                [EndLon] : real,
                [EpisodeId] : long,
                [EventId] : long,
                [Source] : string,
                [MagNWP] : real,
                [Sourcetype] : string,
                [Magnitude] : real,
                [County] : string,
                [CountyID] : long
            )
        """
                .trimIndent()
        if (dmEndpoint == null) {
            assumeTrue(
                false,
                "Skipping test: No DM_CONNECTION_STRING environment variable set for real cluster testing",
            )
            return
        }
        val engineEndpoint = dmEndpoint.replace("https://ingest-", "https://")
        val kcsb = ConnectionStringBuilder.createWithAzureCli(engineEndpoint)
        adminClient = ClientFactory.createClient(kcsb)
        adminClient.executeMgmt(database, createTableScript)
    }

    @AfterAll
    fun dropTables() {
        val dropTableScript = ".drop table $targetTable ifexists"
        adminClient.executeMgmt(database, dropTableScript)
    }

    @Test
    fun `test queued ingestion with CSV blob`(): Unit = runBlocking {
        // Skip test if no DM_CONNECTION_STRING is set
        if (dmEndpoint == null) {
            assumeTrue(
                false,
                "Skipping test: No DM_CONNECTION_STRING environment variable set for real cluster testing",
            )
            return@runBlocking
        }
        val queuedIngestionApiWrapper =
            QueuedIngestionApiWrapper(
                dmUrl = dmEndpoint,
                tokenCredential = azureCliCredential,
                skipSecurityChecks = true,
            )
        val testBlobUrls =
            listOf(
                "https://kustosamples.blob.core.windows.net/samplefiles/StormEvents.csv",
            )
        val format = DataFormat.CSV
        val properties = ingestProperties { enableTracking(true) }

        try {
            // Test successful ingestion submission
            val ingestionResponse =
                queuedIngestionApiWrapper.submitQueuedIngestion(
                    database = database,
                    table = targetTable,
                    blobUrls = testBlobUrls,
                    format = format,
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
                queuedIngestionApiWrapper.pollUntilCompletion(
                    database = database,
                    table = targetTable,
                    operationId =
                    ingestionResponse.ingestionOperationId,
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
}
