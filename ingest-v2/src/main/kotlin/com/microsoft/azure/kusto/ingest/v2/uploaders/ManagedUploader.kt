// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploaders

import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.SimpleRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo

class ManagedUploader(
    override var ignoreSizeLimit: Boolean,
    maxConcurrency: Int,
    maxDataSize: Long? = null,
    configurationCache: ConfigurationCache,
    uploadMethod: UploadMethod = UploadMethod.DEFAULT,
    ingestRetryPolicy: IngestRetryPolicy = SimpleRetryPolicy(),
) :
    ContainerUploaderBase(
        maxConcurrency = maxConcurrency,
        maxDataSize =
        maxDataSize ?: UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES,
        configurationCache = configurationCache,
        uploadMethod = uploadMethod,
        retryPolicy = ingestRetryPolicy,
    ) {
    override suspend fun selectContainers(
        configurationCache: ConfigurationCache,
        uploadMethod: UploadMethod,
    ): List<ContainerInfo> {
        // This method is delegated to and this calls getConfiguration again to ensure fresh data is
        // retrieved
        // or cached data is used as appropriate.
        val containerSettings =
            configurationCache.getConfiguration().containerSettings
                ?: throw IngestException(
                    "No container settings available",
                    isPermanent = true,
                )
        val hasStorage = !containerSettings.containers.isNullOrEmpty()
        val hasLake = !containerSettings.lakeFolders.isNullOrEmpty()

        if (!hasStorage && !hasLake) {
            throw IngestException("No containers available", isPermanent = true)
        }

        // Determine effective upload method
        val effectiveMethod =
            when (uploadMethod) {
                UploadMethod.DEFAULT -> {
                    // Use server's preferred upload method if available
                    val serverPreference =
                        containerSettings.preferredUploadMethod
                    when {
                        serverPreference.equals(
                            "Storage",
                            ignoreCase = true,
                        ) && hasStorage -> {
                            logger.debug(
                                "Using server preferred upload method: Storage",
                            )
                            UploadMethod.STORAGE
                        }
                        serverPreference.equals(
                            "Lake",
                            ignoreCase = true,
                        ) && hasLake -> {
                            logger.debug(
                                "Using server preferred upload method: Lake",
                            )
                            UploadMethod.LAKE
                        }
                        // Fallback: prefer Storage if available, otherwise Lake
                        hasStorage -> {
                            logger.debug(
                                "No server preference or unavailable, defaulting to Storage",
                            )
                            UploadMethod.STORAGE
                        }
                        else -> {
                            logger.debug(
                                "No server preference or unavailable, defaulting to Lake",
                            )
                            UploadMethod.LAKE
                        }
                    }
                }
                UploadMethod.LAKE ->
                    if (hasLake) {
                        UploadMethod.LAKE
                    } else {
                        UploadMethod.STORAGE
                    }
                UploadMethod.STORAGE ->
                    if (hasStorage) {
                        UploadMethod.STORAGE
                    } else {
                        UploadMethod.LAKE
                    }
            }
        return when {
            effectiveMethod == UploadMethod.LAKE && hasLake ->
                containerSettings.lakeFolders
            effectiveMethod == UploadMethod.STORAGE && hasStorage ->
                containerSettings.containers
            hasStorage -> containerSettings.containers
            else -> containerSettings.lakeFolders!!
        }
    }

    override fun close() {}
}
