// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.util.stream.Stream
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConfigurationApiWrapperTest {
    private lateinit var defaultApi: DefaultApi
    private lateinit var wrapper: ConfigurationApiWrapper
    private val clusterUrl = "https://testcluster.kusto.windows.net"
    private val tokenProvider = mockk<TokenCredentialsProvider>(relaxed = true)

    private val logger =
        LoggerFactory.getLogger(ConfigurationApiWrapperTest::class.java)

    private fun endpointAndExceptionClause(): Stream<Arguments?> {
        return Stream.of(
            Arguments.of(
                "Success Scenario",
                System.getenv("DM_CONNECTION_STRING") ?: "https://test.kusto.windows.net",
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
        // Skip test if using default test cluster and no DM_CONNECTION_STRING is set
        val hasRealCluster = System.getenv("DM_CONNECTION_STRING") != null
        if (!isException && !hasRealCluster) {
            assumeTrue(false, "Skipping test: No DM_CONNECTION_STRING environment variable set for real cluster testing")
            return@runBlocking
        }

        val actualTokenProvider =
            AzureCliCredentialBuilder()
                .build() // Replace with a real token provider
        val actualWrapper =
            ConfigurationApiWrapper(cluster, actualTokenProvider, true)

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
            try {
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
            } catch (e: ConnectException) {
                // Skip test if we can't connect to the test cluster due to network issues
                assumeTrue(false, "Skipping test: Unable to connect to test cluster due to network connectivity issues: ${e.message}")
            } catch (e: Exception) {
                if (e.cause is ConnectException) {
                    assumeTrue(false, "Skipping test: Unable to connect to test cluster due to network connectivity issues: ${e.cause?.message}")
                } else {
                    throw e
                }
            }
        }
    }
}
