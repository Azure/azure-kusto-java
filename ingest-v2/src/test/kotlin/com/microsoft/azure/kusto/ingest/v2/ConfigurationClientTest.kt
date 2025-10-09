// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
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
class ConfigurationClientTest :
    IngestV2TestBase(ConfigurationClientTest::class.java) {
    private fun endpointAndExceptionClause(): Stream<Arguments?> {
        return Stream.of(
            Arguments.of(
                "Success Scenario",
                System.getenv("DM_CONNECTION_STRING"),
                false,
                false,
            ),
            // Note on the arg below when this is rolled out to all clusters, this test will
            // start failing
            Arguments.of(
                "Cluster without ingest-v2",
                "https://help.kusto.windows.net",
                true,
                false,
            ),
        )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("endpointAndExceptionClause")
    fun `run e2e test with an actual cluster`(
        testName: String,
        cluster: String,
        isException: Boolean,
        isUnreachableHost: Boolean,
    ): Unit = runBlocking {
        logger.info("Running configuration test {}", testName)
        // val cluster = System.getenv("DM_CONNECTION_STRING")
        val actualWrapper = ConfigurationClient(cluster, tokenProvider, true)
        if (isException) {
            // assert the call to DefaultConfigurationCache throws
            val exception =
                assertThrows<IngestException> {
                    DefaultConfigurationCache(
                        configurationProvider = {
                            actualWrapper
                                .getConfigurationDetails()
                        },
                    )
                        .getConfiguration()
                }
            assertNotNull(exception, "Exception should not be null")
            if (isUnreachableHost) {
                assert(exception.cause is java.net.ConnectException)
                assert(exception.isPermanent == false)
            } else {
                // if the host is reachable, we expect a 404
                assert(exception.failureCode == 404)
                assert(exception.isPermanent == false)
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
