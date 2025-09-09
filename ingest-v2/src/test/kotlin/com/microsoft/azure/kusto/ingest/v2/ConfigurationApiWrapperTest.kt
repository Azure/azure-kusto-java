/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.auth.AzCliTokenCredentialsProvider
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull

class ConfigurationApiWrapperTest {

    @Test
    fun `run e2e test with an actual cluster`(): Unit = runBlocking {
        val actualTokenProvider =
            AzCliTokenCredentialsProvider() // Replace with a real token provider
        val cluster = System.getenv("ENGINE_CONNECTION_STRING")
        val actualWrapper =
            ConfigurationApiWrapper(cluster, actualTokenProvider, true)
        try {
            val config = actualWrapper.getConfigurationDetails()
            println("E2E Test Success: Retrieved configuration: $config")
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
        } catch (ex: Exception) {
            println("E2E Test Failed: ${ex.message}")
            throw ex
        }
    }
}
