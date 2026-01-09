// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.serialization.OffsetDateTimeSerializer
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.time.OffsetDateTime

class ManagedUploaderTest {

    @Test
    fun selectContainers(): Unit = runBlocking {
        val configurationCache = TestConfigurationCache()
        val managedUploader =
            ManagedUploaderBuilder.create()
                .withConfigurationCache(configurationCache)
                .build()
        val selectedContainers =
            managedUploader.selectContainers(UploadMethod.DEFAULT)
        assertNotNull(selectedContainers)
        assertTrue(selectedContainers.isNotEmpty())
        selectedContainers.forEach {
            assertNotNull(it.containerInfo.path)
            assertTrue(it.containerInfo.path?.contains("alakefolder") ?: false)
            assertFalse(
                it.containerInfo.path?.contains("somecontainer") ?: true,
            )
        }
    }

    private class TestConfigurationCache : ConfigurationCache {
        private val json = Json {
            ignoreUnknownKeys = true
            serializersModule = SerializersModule {
                contextual(OffsetDateTime::class, OffsetDateTimeSerializer)
            }
        }
        override val refreshInterval: Duration
            get() = Duration.ofHours(1)

        override suspend fun getConfiguration(): ConfigurationResponse {
            val resourcesDirectory = "src/test/resources/"
            val fileName = "config-response.json"
            val configContent =
                withContext(Dispatchers.IO) {
                    Files.readString(
                        Paths.get(resourcesDirectory + fileName),
                        StandardCharsets.UTF_8,
                    )
                }
            val configurationResponse =
                json.decodeFromString<ConfigurationResponse>(configContent)

            assertNotNull(configurationResponse)
            assertNotNull(configurationResponse.containerSettings)
            return configurationResponse
        }

        override fun close() {
            // No resources to clean up in this test implementation
        }
    }
}
