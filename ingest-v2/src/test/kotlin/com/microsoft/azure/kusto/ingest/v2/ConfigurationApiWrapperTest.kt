/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.auth.AzCliTokenCredentialsProvider
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.slf4j.LoggerFactory

class ConfigurationApiWrapperTest {

    private val logger =
        LoggerFactory.getLogger(ConfigurationApiWrapperTest::class.java)

    @Test
    fun `run e2e test with an actual cluster`(): Unit = runBlocking {
        val actualTokenProvider =
            AzCliTokenCredentialsProvider() // Replace with a real token provider
        val cluster = System.getenv("DM_CONNECTION_STRING")
        val actualWrapper =
            ConfigurationApiWrapper(cluster, actualTokenProvider, true)
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
            assertNotNull(
                defaultCachedConfig,
                "Configuration should not be null",
            )
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
        } catch (ex: Exception) {
            logger.error("E2E Test Failed", ex)
            throw ex
        }
    }
}
