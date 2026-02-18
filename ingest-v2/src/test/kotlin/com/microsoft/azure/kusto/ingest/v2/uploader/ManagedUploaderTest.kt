// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.microsoft.azure.kusto.ingest.v2.common.CachedConfigurationData
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.serialization.OffsetDateTimeSerializer
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo
import com.microsoft.azure.kusto.ingest.v2.models.ContainerSettings
import com.microsoft.azure.kusto.ingest.v2.models.IngestionSettings
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.time.OffsetDateTime

class ManagedUploaderTest {

    companion object {
        private val STORAGE_CONTAINER = ContainerInfo(
            path = "https://somecontainer.z11.blob.storage.azure.net/trdwvweg9nfnngghb1eey-20260108-ingestdata-e5c334ee145d4b4-0?sv=keys"
        )
        private val LAKE_CONTAINER = ContainerInfo(
            path = "https://alakefolder.onelake.fabric.microsoft.com/17a97d10-a17f-4d72-8f38-858aac992978/bb9c26d4-4f99-44b5-9614-3ebb037f3510/Ingestions/20260108-lakedata"
        )
    }

    // ===== Both containers available (from config-response.json with preferredUploadMethod=Lake) =====

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

    // ===== Only storage containers available =====

    @ParameterizedTest(name = "OnlyStorage_PreferredUploadMethod={0}")
    @CsvSource("DEFAULT", "STORAGE", "LAKE")
    fun selectContainers_onlyStorageAvailable_returnsStorage(preferredUploadMethod: String): Unit = runBlocking {
        val uploadMethod = UploadMethod.valueOf(preferredUploadMethod)
        val cache = buildCache(
            containers = listOf(STORAGE_CONTAINER),
            lakeFolders = null,
            preferredUploadMethod = "Storage",
        )
        val managedUploader = ManagedUploaderBuilder.create()
            .withConfigurationCache(cache)
            .build()

        val selectedContainers = managedUploader.selectContainers(uploadMethod)

        assertNotNull(selectedContainers)
        assertTrue(selectedContainers.isNotEmpty())
        selectedContainers.forEach {
            assertTrue(it.containerInfo.path?.contains("somecontainer") ?: false)
            assertTrue(it.uploadMethod == UploadMethod.STORAGE)
        }
    }

    // ===== Only lake containers available =====

    @ParameterizedTest(name = "OnlyLake_PreferredUploadMethod={0}")
    @CsvSource("DEFAULT", "STORAGE", "LAKE")
    fun selectContainers_onlyLakeAvailable_returnsLake(preferredUploadMethod: String): Unit = runBlocking {
        val uploadMethod = UploadMethod.valueOf(preferredUploadMethod)
        val cache = buildCache(
            containers = null,
            lakeFolders = listOf(LAKE_CONTAINER),
            preferredUploadMethod = "Lake",
        )
        val managedUploader = ManagedUploaderBuilder.create()
            .withConfigurationCache(cache)
            .build()

        val selectedContainers = managedUploader.selectContainers(uploadMethod)

        assertNotNull(selectedContainers)
        assertTrue(selectedContainers.isNotEmpty())
        selectedContainers.forEach {
            assertTrue(it.containerInfo.path?.contains("alakefolder") ?: false)
            assertTrue(it.uploadMethod == UploadMethod.LAKE)
        }
    }

    // ===== Neither containers available =====

    @Test
    fun selectContainers_noContainersAvailable_throws(): Unit = runBlocking {
        val cache = buildCache(
            containers = null,
            lakeFolders = null,
            preferredUploadMethod = null,
        )
        val managedUploader = ManagedUploaderBuilder.create()
            .withConfigurationCache(cache)
            .build()

        val exception = assertThrows<IngestException> {
            managedUploader.selectContainers(UploadMethod.DEFAULT)
        }
        assertTrue(exception.isPermanent == true)
    }

    // ===== No container settings at all =====

    @Test
    fun selectContainers_noContainerSettings_throws(): Unit = runBlocking {
        val cache = InlineConfigurationCache(
            CachedConfigurationData(
                ConfigurationResponse(
                    containerSettings = null,
                    ingestionSettings = IngestionSettings(maxBlobsPerBatch = 20, maxDataSize = 6442450944),
                )
            )
        )
        val managedUploader = ManagedUploaderBuilder.create()
            .withConfigurationCache(cache)
            .build()

        val exception = assertThrows<IngestException> {
            managedUploader.selectContainers(UploadMethod.DEFAULT)
        }
        assertTrue(exception.isPermanent == true)
    }

    // ===== Both available, server prefers Storage =====

    @Test
    fun selectContainers_bothAvailable_serverPrefersStorage_defaultUsesStorage(): Unit = runBlocking {
        val cache = buildCache(
            containers = listOf(STORAGE_CONTAINER),
            lakeFolders = listOf(LAKE_CONTAINER),
            preferredUploadMethod = "Storage",
        )
        val managedUploader = ManagedUploaderBuilder.create()
            .withConfigurationCache(cache)
            .build()

        val selectedContainers = managedUploader.selectContainers(UploadMethod.DEFAULT)

        assertNotNull(selectedContainers)
        assertTrue(selectedContainers.isNotEmpty())
        selectedContainers.forEach {
            assertTrue(it.containerInfo.path?.contains("somecontainer") ?: false)
            assertTrue(it.uploadMethod == UploadMethod.STORAGE)
        }
    }

    // ===== Both available, server preference is null =====

    @Test
    fun selectContainers_bothAvailable_serverPreferenceNull_defaultUsesStorage(): Unit = runBlocking {
        val cache = buildCache(
            containers = listOf(STORAGE_CONTAINER),
            lakeFolders = listOf(LAKE_CONTAINER),
            preferredUploadMethod = null,
        )
        val managedUploader = ManagedUploaderBuilder.create()
            .withConfigurationCache(cache)
            .build()

        val selectedContainers = managedUploader.selectContainers(UploadMethod.DEFAULT)

        assertNotNull(selectedContainers)
        assertTrue(selectedContainers.isNotEmpty())
        selectedContainers.forEach {
            assertTrue(it.containerInfo.path?.contains("somecontainer") ?: false)
            assertTrue(it.uploadMethod == UploadMethod.STORAGE)
        }
    }

    // ===== Both available, explicit LAKE overrides server preference =====

    @Test
    fun selectContainers_bothAvailable_serverPrefersStorage_explicitLakeUsesLake(): Unit = runBlocking {
        val cache = buildCache(
            containers = listOf(STORAGE_CONTAINER),
            lakeFolders = listOf(LAKE_CONTAINER),
            preferredUploadMethod = "Storage",
        )
        val managedUploader = ManagedUploaderBuilder.create()
            .withConfigurationCache(cache)
            .build()

        val selectedContainers = managedUploader.selectContainers(UploadMethod.LAKE)

        assertNotNull(selectedContainers)
        assertTrue(selectedContainers.isNotEmpty())
        selectedContainers.forEach {
            assertTrue(it.containerInfo.path?.contains("alakefolder") ?: false)
            assertTrue(it.uploadMethod == UploadMethod.LAKE)
        }
    }

    // ===== Both available, explicit STORAGE overrides server preference =====

    @Test
    fun selectContainers_bothAvailable_serverPrefersLake_explicitStorageUsesStorage(): Unit = runBlocking {
        val cache = buildCache(
            containers = listOf(STORAGE_CONTAINER),
            lakeFolders = listOf(LAKE_CONTAINER),
            preferredUploadMethod = "Lake",
        )
        val managedUploader = ManagedUploaderBuilder.create()
            .withConfigurationCache(cache)
            .build()

        val selectedContainers = managedUploader.selectContainers(UploadMethod.STORAGE)

        assertNotNull(selectedContainers)
        assertTrue(selectedContainers.isNotEmpty())
        selectedContainers.forEach {
            assertTrue(it.containerInfo.path?.contains("somecontainer") ?: false)
            assertTrue(it.uploadMethod == UploadMethod.STORAGE)
        }
    }

    // ===== Helper methods =====

    private fun buildCache(
        containers: List<ContainerInfo>?,
        lakeFolders: List<ContainerInfo>?,
        preferredUploadMethod: String?,
    ): ConfigurationCache {
        return InlineConfigurationCache(
            CachedConfigurationData(
                ConfigurationResponse(
                    containerSettings = ContainerSettings(
                        containers = containers,
                        lakeFolders = lakeFolders,
                        refreshInterval = "01:00:00",
                        preferredUploadMethod = preferredUploadMethod,
                    ),
                    ingestionSettings = IngestionSettings(
                        maxBlobsPerBatch = 20,
                        maxDataSize = 6442450944,
                    ),
                )
            )
        )
    }

    private class InlineConfigurationCache(
        private val data: CachedConfigurationData,
    ) : ConfigurationCache {
        override val refreshInterval: Duration = Duration.ofHours(1)
        override suspend fun getConfiguration(): CachedConfigurationData = data
        override fun close() {}
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