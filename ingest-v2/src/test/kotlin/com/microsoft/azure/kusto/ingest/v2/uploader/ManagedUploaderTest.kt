// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.microsoft.azure.kusto.ingest.v2.common.CachedConfigurationData
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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.time.OffsetDateTime

class ManagedUploaderTest {

    @ParameterizedTest(name = "PreferredUploadMethod={0}")
    @CsvSource("DEFAULT", "STORAGE", "LAKE")
    fun selectContainers(preferredUploadMethod: String): Unit = runBlocking {
        val uploadMethod = UploadMethod.valueOf(preferredUploadMethod)
        val configurationCache = TestConfigurationCache()
        val managedUploader =
            ManagedUploaderBuilder.create()
                .withConfigurationCache(configurationCache)
                .build()
        val selectedContainers = managedUploader.selectContainers(uploadMethod)
        assertNotNull(selectedContainers)
        assertTrue(selectedContainers.isNotEmpty())
        selectedContainers.forEach {
            assertNotNull(it.containerInfo.path)
            // When the server configuration prefers Lake and the user does not specify (DEFAULT),
            // ManagedUploader should honor the server preference and use Lake. If the user
            // explicitly
            // specifies a method (e.g., STORAGE), that explicit choice is respected.
            if (uploadMethod != UploadMethod.STORAGE) {
                assertTrue(
                    it.containerInfo.path?.contains("alakefolder") ?: false,
                )
                assertFalse(
                    it.containerInfo.path?.contains("somecontainer")
                        ?: false,
                )
            } else {
                // User mentioned storage here, use that
                assertFalse(
                    it.containerInfo.path?.contains("alakefolder") ?: false,
                )
                assertTrue(
                    it.containerInfo.path?.contains("somecontainer")
                        ?: false,
                )
            }
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

        override suspend fun getConfiguration(): CachedConfigurationData {
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
            return CachedConfigurationData(configurationResponse)
        }

        override fun close() {
            // No resources to clean up in this test implementation
        }
    }
}
