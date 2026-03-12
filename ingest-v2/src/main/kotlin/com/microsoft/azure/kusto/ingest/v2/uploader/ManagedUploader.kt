// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.SimpleRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException

class ManagedUploader
internal constructor(
    override var ignoreSizeLimit: Boolean,
    maxConcurrency: Int,
    maxDataSize: Long,
    configurationCache: ConfigurationCache,
    uploadMethod: UploadMethod = UploadMethod.DEFAULT,
    ingestRetryPolicy: IngestRetryPolicy = SimpleRetryPolicy(),
    tokenCredential: TokenCredential? = null,
) :
    ContainerUploaderBase(
        maxConcurrency = maxConcurrency,
        maxDataSize = maxDataSize,
        configurationCache = configurationCache,
        uploadMethod = uploadMethod,
        retryPolicy = ingestRetryPolicy,
        tokenCredential = tokenCredential,
    ) {

    companion object {
        /**
         * Creates a new builder for constructing ManagedUploader instances.
         *
         * @return a new ManagedUploaderBuilder instance
         */
        @JvmStatic
        fun builder(): ManagedUploaderBuilder {
            return ManagedUploaderBuilder.create()
        }
    }

    override suspend fun selectContainers(
        uploadMethod: UploadMethod,
    ): RoundRobinContainerList {
        // This method is delegated to and this calls getConfiguration again to ensure fresh data is
        // retrieved or cached data is used as appropriate.
        val configuration = configurationCache.getConfiguration()
        val containerSettings =
            configuration.containerSettings
                ?: throw IngestException(
                    "No container settings available",
                    isPermanent = true,
                )
        val hasStorage = !containerSettings.containers.isNullOrEmpty()
        val hasLake = !containerSettings.lakeFolders.isNullOrEmpty()

        logger.debug(
            "Selecting containers: preferred upload method: {}, has storage containers: {}, has lake folders: {}",
            uploadMethod,
            hasStorage,
            hasLake,
        )

        // No containers available at all
        if (!hasStorage && !hasLake) {
            throw IngestException("No containers available", isPermanent = true)
        }

        // Only lake containers available
        if (!hasStorage) {
            logger.debug("Only lake containers available, using lake folders")
            return configuration.lakeContainerList
        }

        // Only storage containers available
        if (!hasLake) {
            logger.debug(
                "Only storage containers available, using storage containers",
            )
            return configuration.storageContainerList
        }

        // Both types available - determine effective upload method.
        // If user specified DEFAULT, use the server's preferred method (defaulting to Storage).
        // Otherwise, use the user's explicit choice directly.
        val effectiveMethod =
            if (uploadMethod == UploadMethod.DEFAULT) {
                val serverPreference =
                    containerSettings.preferredUploadMethod
                if (serverPreference.equals("Lake", ignoreCase = true)) {
                    UploadMethod.LAKE
                } else {
                    UploadMethod.STORAGE
                }
            } else {
                uploadMethod
            }

        logger.debug(
            "Selected {} containers based on effective method: {}",
            if (effectiveMethod == UploadMethod.LAKE) "lake" else "storage",
            effectiveMethod,
        )

        // Return the appropriate RoundRobinContainerList from the configuration cache.
        // The cache maintains shared counters for each container type to ensure
        // even distribution across all uploaders sharing the same cache.
        return if (effectiveMethod == UploadMethod.LAKE) {
            configuration.lakeContainerList
        } else {
            configuration.storageContainerList
        }
    }

    override fun close() {}
}
