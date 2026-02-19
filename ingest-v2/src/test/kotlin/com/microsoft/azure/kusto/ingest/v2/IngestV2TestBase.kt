// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.data.Client
import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.ingest.v2.models.Format
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

abstract class IngestV2TestBase(testClass: Class<*>) {
    protected val logger: Logger = LoggerFactory.getLogger(testClass)

    // Shared across all test class instances via CachingTokenCredential singleton.
    // Ensures az account get-access-token is invoked only once per scope.
    protected val tokenProvider: TokenCredential = CachingTokenCredential.INSTANCE

    protected val database = System.getenv("TEST_DATABASE") ?: "e2e"
    protected val dmEndpoint: String =
        System.getenv("DM_CONNECTION_STRING")
            ?: throw IllegalArgumentException(
                "DM_CONNECTION_STRING environment variable is not set",
            )
    protected val oneLakeFolder: String? = System.getenv("ONE_LAKE_FOLDER")
    protected val targetTestFormat = Format.json
    protected val engineEndpoint: String =
        IngestClientBase.getQueryEndpoint(dmEndpoint) ?: dmEndpoint
    lateinit var targetTable: String
    protected val columnNamesToTypes: Map<String, String> =
        mapOf(
            "timestamp" to "datetime",
            "deviceId" to "guid",
            "messageId" to "guid",
            "temperature" to "real",
            "humidity" to "real",
            "format" to "string",
            "SourceLocation" to "string",
            "Type" to "string",
        )
    protected lateinit var adminClusterClient: Client

    @BeforeEach
    fun createTables() {
        targetTable =
            "IngestV2Test_${UUID.randomUUID().toString().replace("-", "_")}"
        val createTableScript =
            """
            .create-merge table $targetTable (
                ${columnNamesToTypes.entries.joinToString(",") { "['${it.key}']:${it.value}" }}
            )
            """
                .trimIndent()
        val mappingReference =
            """
            .create-or-alter table $targetTable ingestion json mapping '${targetTable}_mapping' ```[
            ${
                columnNamesToTypes.keys.joinToString("\n") { col ->
                    when (col) {
                        "SourceLocation" -> "    {\"column\":\"$col\", \"Properties\":{\"Transform\":\"SourceLocation\"}},"
                        "Type" -> "    {\"column\":\"$col\", \"Properties\":{\"ConstValue\":\"MappingRef\"}}"
                        else -> "    {\"column\":\"$col\", \"Properties\":{\"Path\":\"$.$col\"}},"
                    }
                }.removeSuffix(",")
            }
           ]```
            """
                .trimIndent()
        adminClusterClient =
            ClientFactory.createClient(
                ConnectionStringBuilder.createWithAzureCli(
                    engineEndpoint,
                ),
            )
        adminClusterClient.executeMgmt(database, createTableScript)
        adminClusterClient.executeMgmt(database, mappingReference)
        clearDatabaseSchemaCache()

        // Allow subclasses to perform additional setup
        additionalSetup()
    }

    /**
     * Hook method for subclasses to perform additional setup after table
     * creation. By default, does nothing. Streaming test classes can override
     * to enable streaming policy.
     */
    protected open fun additionalSetup() {
        // Default: no additional setup
    }

    protected fun alterTableToEnableStreaming() {
        adminClusterClient.executeMgmt(
            database,
            ".alter table $targetTable policy streamingingestion enable",
        )
    }

    protected fun clearDatabaseSchemaCache() {
        adminClusterClient.executeMgmt(
            database,
            ".clear database cache streamingingestion schema",
        )
    }

    @AfterEach
    fun dropTables() {
        val dropTableScript = ".drop table $targetTable ifexists"
        logger.info("Dropping table $targetTable")
        adminClusterClient.executeMgmt(database, dropTableScript)
    }

    protected fun awaitAndQuery(
        query: String,
        queryColumnName: String = "count",
        expectedResultsCount: Long,
        isManagementQuery: Boolean = false,
        testName: String,
    ) {
        Awaitility.await()
            .atMost(Duration.of(3, ChronoUnit.MINUTES))
            .pollInterval(Duration.of(5, ChronoUnit.SECONDS))
            .ignoreExceptions()
            .alias("Awaiting query result: $query on test: $testName")
            .untilAsserted {
                val results =
                    if (isManagementQuery) {
                        adminClusterClient
                            .executeMgmt(database, query)
                            .primaryResults
                    } else {
                        adminClusterClient
                            .executeQuery(database, query)
                            .primaryResults
                    }
                results.next()
                val actualResultCount = results.getLong(queryColumnName)
                logger.trace(
                    "For query {} , Current result count: {}, waiting for {}",
                    query,
                    actualResultCount,
                    expectedResultsCount,
                )
                actualResultCount >= expectedResultsCount
                assertNotNull(results, "Query results should not be null")
                assertNotNull(actualResultCount, "Count should not be null")
                assertTrue(
                    actualResultCount >= expectedResultsCount,
                    "expected $expectedResultsCount counts should match $actualResultCount",
                )
            }
    }
}