// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.container

import com.azure.core.util.Context
import com.azure.storage.blob.BlobClientBuilder
import com.azure.storage.blob.models.BlockBlobItem
import com.azure.storage.blob.models.ParallelTransferOptions
import com.azure.storage.blob.options.BlobParallelUploadOptions
import com.azure.storage.common.policy.RequestRetryOptions
import com.azure.storage.common.policy.RetryPolicyType
import com.microsoft.azure.kusto.ingest.v2.BLOB_UPLOAD_TIMEOUT_HOURS
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_BLOCK_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_MAX_SINGLE_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_RETRY_DELAY_MS
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_RETRY_MAX_DELAY_MS
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_RETRY_MAX_TRIES
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_RETRY_TIMEOUT_SECONDS
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.time.Duration
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger

private val DEFAULT_RETRY_OPTIONS =
    RequestRetryOptions(
        RetryPolicyType.EXPONENTIAL,
        UPLOAD_RETRY_MAX_TRIES,
        UPLOAD_RETRY_TIMEOUT_SECONDS,
        UPLOAD_RETRY_DELAY_MS,
        UPLOAD_RETRY_MAX_DELAY_MS,
        null,
    )

enum class UploadMethod {
    DEFAULT,    // Use server preference or Storage as fallback
    STORAGE,    // Use Storage blob
    LAKE        // Use OneLake
}

data class UploadSource(
    val name: String,
    val stream: InputStream,
    val sizeBytes: Long = -1
)

class BlobUploadContainer(
    val configurationCache: ConfigurationCache,
    private val uploadMethod: UploadMethod = UploadMethod.DEFAULT,
    private val maxRetries: Int = 3,
    private val maxDataSize: Long = 4L * 1024 * 1024 * 1024, // 4GB default
    private val ignoreSizeLimit: Boolean = false,
    private val maxConcurrency: Int = 4
) : UploadContainerBase {
    private val logger = LoggerFactory.getLogger(BlobUploadContainer::class.java)
    private val containerIndex = AtomicInteger(0)

    override suspend fun uploadAsync(name: String, stream: InputStream): String {
        val errorCode = validateStream(stream, name)
        if (errorCode != null) {
            logger.error("Stream validation failed for {}: {}", name, errorCode.description)
            throw IngestException(errorCode.description, isPermanent = true)
        }

        if (!ignoreSizeLimit && stream.available() > 0) {
            val availableSize = stream.available().toLong()
            if (availableSize > maxDataSize) {
                logger.error(
                    "Stream size {} exceeds max allowed size {} for: {}",
                    availableSize,
                    maxDataSize,
                    name
                )
                throw IngestException(
                    "Upload source exceeds maximum allowed size: $availableSize > $maxDataSize",
                    isPermanent = true
                )
            }
        }

        val containers = selectContainers()
        require(containers.isNotEmpty()) { "No containers available for upload" }

        var lastException: Exception? = null

        repeat(maxRetries) { attempt ->
            val container = containers[containerIndex.getAndIncrement() % containers.size]

            try {
                return uploadToContainer(name, stream, container)
            } catch (e: Exception) {
                logger.warn("Upload attempt ${attempt + 1} failed for container ${container.path}", e)
                lastException = e

                if (stream.markSupported()) {
                    try {
                        stream.reset()
                    } catch (resetEx: Exception) {
                        logger.warn("Failed to reset stream for retry", resetEx)
                        throw IngestException(
                            "Upload failed and stream cannot be reset for retry",
                            cause = e,
                            isPermanent = true
                        )
                    }
                }
            }
        }

        throw IngestException(
            "Failed to upload after $maxRetries attempts",
            cause = lastException,
            isPermanent = false
        )
    }

    suspend fun uploadManyAsync(sources: List<UploadSource>): UploadResults = coroutineScope {
        logger.info("Starting batch upload of {} sources with max concurrency {}", sources.size, maxConcurrency)

        // Process sources in chunks to respect maxConcurrency at file level
        val results = sources.chunked(maxConcurrency).flatMap { chunk ->
            chunk.map { source ->
                async {
                    val startedAt = OffsetDateTime.now()
                    try {
                        val blobUrl = uploadAsync(source.name, source.stream)
                        val completedAt = OffsetDateTime.now()
                        UploadResult.Success(
                            sourceName = source.name,
                            startedAt = startedAt,
                            completedAt = completedAt,
                            blobUrl = blobUrl,
                            sizeBytes = source.sizeBytes
                        )
                    } catch (e: Exception) {
                        val completedAt = OffsetDateTime.now()
                        val errorCode = when {
                            e.message?.contains("size") == true -> UploadErrorCode.SOURCE_SIZE_LIMIT_EXCEEDED
                            e.message?.contains("readable") == true -> UploadErrorCode.SOURCE_NOT_READABLE
                            e.message?.contains("empty") == true -> UploadErrorCode.SOURCE_IS_EMPTY
                            e.message?.contains("container") == true -> UploadErrorCode.NO_CONTAINERS_AVAILABLE
                            else -> UploadErrorCode.UPLOAD_FAILED
                        }

                        UploadResult.Failure(
                            sourceName = source.name,
                            startedAt = startedAt,
                            completedAt = completedAt,
                            errorCode = errorCode,
                            errorMessage = e.message ?: "Upload failed",
                            exception = e,
                            isPermanent = e is IngestException && e.isPermanent == true
                        )
                    }
                }
            }.awaitAll()
        }

        val successes = results.filterIsInstance<UploadResult.Success>()
        val failures = results.filterIsInstance<UploadResult.Failure>()

        logger.info(
            "Batch upload completed: {} successes, {} failures out of {} total",
            successes.size,
            failures.size,
            sources.size
        )

        UploadResults(successes, failures)
    }

    private fun validateStream(stream: InputStream, name: String): UploadErrorCode? {
        return try {
            if (stream.available() < 0) {
                UploadErrorCode.SOURCE_NOT_READABLE
            } else if (stream.markSupported() && stream.available() == 0) {
                UploadErrorCode.SOURCE_IS_EMPTY
            } else {
                null
            }
        } catch (e: Exception) {
            logger.warn("Error validating stream for {}", name, e)
            UploadErrorCode.SOURCE_NOT_READABLE
        }
    }

    private suspend fun uploadToContainer(
        name: String,
        stream: InputStream,
        container: ContainerInfo
    ): String {
        val (url, sas) = container.path!!.split("?", limit = 2)

        val blobClient = BlobClientBuilder()
            .endpoint(container.path)
            .blobName(name)
            .buildClient()

        logger.debug("Uploading stream to blob url: {} to container {}", url, name)

        val parallelTransferOptions = ParallelTransferOptions()
            .setBlockSizeLong(UPLOAD_BLOCK_SIZE_BYTES)
            .setMaxConcurrency(maxConcurrency)
            .setMaxSingleUploadSizeLong(UPLOAD_MAX_SINGLE_SIZE_BYTES)

        val blobUploadOptions = BlobParallelUploadOptions(stream)
            .setParallelTransferOptions(parallelTransferOptions)

        val blobUploadResult = blobClient.uploadWithResponse(
            blobUploadOptions,
            Duration.ofHours(BLOB_UPLOAD_TIMEOUT_HOURS),
            Context.NONE,
        )

        return if (blobUploadResult.statusCode in 200..299 && blobUploadResult.value != null) {
            val blockBlobItem: BlockBlobItem = blobUploadResult.value
            logger.debug(
                "Upload succeeded to blob url: {} with eTag: {}",
                url,
                blockBlobItem.eTag,
            )
            "$url/$name?$sas"
        } else {
            throw IngestException(
                "Upload failed with status: ${blobUploadResult.statusCode}",
                isPermanent = blobUploadResult.statusCode >= 400 && blobUploadResult.statusCode < 500
            )
        }
    }

    private suspend fun selectContainers(): List<ContainerInfo> {
        val configResponse = configurationCache.getConfiguration()
        val containerSettings = configResponse.containerSettings
            ?: throw IngestException("No container settings available", isPermanent = true)

        val hasStorage = !containerSettings.containers.isNullOrEmpty()
        val hasLake = !containerSettings.lakeFolders.isNullOrEmpty()

        if (!hasStorage && !hasLake) {
            throw IngestException("No containers available", isPermanent = true)
        }

        // Determine effective upload method
        val effectiveMethod = when (uploadMethod) {
            UploadMethod.DEFAULT -> {
                // Use server's preferred upload method if available
                val serverPreference = containerSettings.preferredUploadMethod
                when {
                    serverPreference.equals("Storage", ignoreCase = true) && hasStorage -> {
                        logger.debug("Using server preferred upload method: Storage")
                        UploadMethod.STORAGE
                    }
                    serverPreference.equals("Lake", ignoreCase = true) && hasLake -> {
                        logger.debug("Using server preferred upload method: Lake")
                        UploadMethod.LAKE
                    }
                    // Fallback: prefer Storage if available, otherwise Lake
                    hasStorage -> {
                        logger.debug("No server preference or unavailable, defaulting to Storage")
                        UploadMethod.STORAGE
                    }
                    else -> {
                        logger.debug("No server preference or unavailable, defaulting to Lake")
                        UploadMethod.LAKE
                    }
                }
            }
            UploadMethod.LAKE -> if (hasLake) UploadMethod.LAKE else UploadMethod.STORAGE
            UploadMethod.STORAGE -> if (hasStorage) UploadMethod.STORAGE else UploadMethod.LAKE
        }

        return when {
            effectiveMethod == UploadMethod.LAKE && hasLake -> containerSettings.lakeFolders!!
            effectiveMethod == UploadMethod.STORAGE && hasStorage -> containerSettings.containers!!
            hasStorage -> containerSettings.containers!!
            else -> containerSettings.lakeFolders!!
        }
    }
}
