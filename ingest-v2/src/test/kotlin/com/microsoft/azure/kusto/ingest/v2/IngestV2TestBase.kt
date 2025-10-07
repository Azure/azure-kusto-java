// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.azure.identity.AzureCliCredentialBuilder
import com.microsoft.azure.kusto.data.Client
import com.microsoft.azure.kusto.data.ClientFactory
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder
import com.microsoft.azure.kusto.ingest.v2.models.Format
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class IngestV2TestBase(testClass: Class<*>) {
    protected val logger: Logger = LoggerFactory.getLogger(testClass)
    protected val tokenProvider: TokenCredential =
        AzureCliCredentialBuilder().build()
    protected val database = System.getenv("TEST_DATABASE") ?: "e2e"
    protected val dmEndpoint: String =
        System.getenv("DM_CONNECTION_STRING")
            ?: throw IllegalArgumentException(
                "DM_CONNECTION_STRING environment variable is not set",
            )
    protected val targetTestFormat = Format.json
    protected val engineEndpoint: String =
        dmEndpoint.replace("https://ingest-", "https://")
    protected open val targetTable: String =
        "Storms_${UUID.randomUUID().toString().replace("-", "").take(8)}"
    protected open val columnNamesToTypes: Map<String, String> = emptyMap()
    protected lateinit var adminClient: Client

    @BeforeAll
    open fun createTables() {
        if (columnNamesToTypes.isEmpty()) return
        val createTableScript =
            """
            .create table $targetTable (
                ${columnNamesToTypes.entries.joinToString(",") { "['${it.key}']:${it.value}" }}
            )
            """
                .trimIndent()
        val mappingReference =
            """
            .create table $targetTable ingestion csv mapping '${targetTable}_mapping' ```[
${columnNamesToTypes.keys.mapIndexed { idx, col ->
                when (col) {
                    "SourceLocation" -> "    {\"column\":\"$col\", \"Properties\":{\"Transform\":\"SourceLocation\"}},"
                    "Type" -> "    {\"column\":\"$col\", \"Properties\":{\"ConstValue\":\"MappingRef\"}}"
                    else -> "    {\"column\":\"$col\", \"Properties\":{\"Ordinal\":\"$idx\"}},"
                }
            }.joinToString("\n").removeSuffix(",")}
           ]```
            """
                .trimIndent()
        val engineEndpoint = dmEndpoint.replace("https://ingest-", "https://")
        val kcsb = ConnectionStringBuilder.createWithAzureCli(engineEndpoint)
        adminClient = ClientFactory.createClient(kcsb)
        adminClient.executeMgmt(database, createTableScript)
        adminClient.executeMgmt(database, mappingReference)
    }

    protected fun enableStreamingIngestion(waitTimeMs: Long = 30000) {
        if (!::adminClient.isInitialized) {
            throw IllegalStateException(
                "adminClient not initialized. Call createTables() first.",
            )
        }

        adminClient.executeMgmt(
            database,
            ".alter table $targetTable policy streamingingestion enable",
        )
        logger.info(
            "Enabled streaming ingestion policy on table {}",
            targetTable,
        )

        Thread.sleep(waitTimeMs)
        logger.info(
            "Waited {} ms for streaming ingestion policy to propagate",
            waitTimeMs,
        )
    }

    @AfterAll
    open fun dropTables() {
        if (!::adminClient.isInitialized) return
        val dropTableScript = ".drop table $targetTable ifexists"
        logger.error("Dropping table $targetTable")
        adminClient.executeMgmt(database, dropTableScript)
    }
}
