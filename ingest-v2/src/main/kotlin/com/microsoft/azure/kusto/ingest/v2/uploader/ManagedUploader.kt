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
        configurationCache: ConfigurationCache,
        uploadMethod: UploadMethod,
    ): List<ExtendedContainerInfo> {
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
                containerSettings.lakeFolders.map {
                    ExtendedContainerInfo(it, UploadMethod.LAKE)
                }
            effectiveMethod == UploadMethod.STORAGE && hasStorage ->
                containerSettings.containers.map {
                    ExtendedContainerInfo(it, UploadMethod.STORAGE)
                }
            hasStorage ->
                containerSettings.containers.map {
                    ExtendedContainerInfo(it, UploadMethod.STORAGE)
                }
            else ->
                containerSettings.lakeFolders!!.map {
                    ExtendedContainerInfo(it, UploadMethod.LAKE)
                }
        }
    }

    override fun close() {}
}
