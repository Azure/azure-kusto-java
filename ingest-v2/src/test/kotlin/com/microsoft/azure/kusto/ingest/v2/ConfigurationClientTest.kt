// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.identity.AzureCliCredentialBuilder
import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.slf4j.LoggerFactory
import java.util.stream.Stream
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConfigurationClientTest {
    private val logger =
        LoggerFactory.getLogger(ConfigurationClientTest::class.java)

    private fun endpointAndExceptionClause(): Stream<Arguments?> {
        return Stream.of(
            Arguments.of(System.getenv("DM_CONNECTION_STRING"), false),
            Arguments.of("https://help.kusto.windows.net", true),
        )
    }

    @ParameterizedTest
    @MethodSource("endpointAndExceptionClause")
    fun `run e2e test with an actual cluster`(
        cluster: String,
        isException: Boolean,
    ): Unit = runBlocking {
        val actualTokenProvider =
            AzureCliCredentialBuilder()
                .build() // Replace with a real token provider
        // val cluster = System.getenv("DM_CONNECTION_STRING")
        val actualWrapper =
            ConfigurationClient(cluster, actualTokenProvider, true)
        if (isException) {
            // assert the call to DefaultConfigurationCache throws
            assertThrows<IngestException> {
                DefaultConfigurationCache(
                    configurationProvider = {
                        actualWrapper.getConfigurationDetails()
                    },
                )
                    .getConfiguration()
            }
        } else {
            val defaultCachedConfig =
                DefaultConfigurationCache(
                    configurationProvider = {
                        actualWrapper.getConfigurationDetails()
                    },
                )
            logger.debug(
                "E2E Test Success: Retrieved configuration: {}",
                defaultCachedConfig,
            )
            assertNotNull(
                defaultCachedConfig,
                "DefaultConfiguration should not be null",
            )
            val config = defaultCachedConfig.getConfiguration()
            assertNotNull(config, "Configuration should not be null")
            assertNotNull(
                config.containerSettings,
                "ContainerSettings should not be null",
            )
            assertNotNull(
                config.containerSettings.preferredUploadMethod,
                "Preferred upload should not be null",
            )
            config.containerSettings.containers?.forEach { containerInfo ->
                run {
                    assertNotNull(
                        containerInfo.path,
                        "Container path should not be null",
                    )
                }
            }
        }
    }
}
