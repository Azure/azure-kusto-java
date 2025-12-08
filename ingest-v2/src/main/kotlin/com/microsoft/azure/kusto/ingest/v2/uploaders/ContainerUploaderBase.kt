// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploaders

import com.azure.core.util.Context
import com.azure.storage.blob.BlobClientBuilder
import com.azure.storage.blob.models.BlockBlobItem
import com.azure.storage.blob.models.ParallelTransferOptions
import com.azure.storage.blob.options.BlobParallelUploadOptions
import com.microsoft.azure.kusto.ingest.v2.BLOB_UPLOAD_TIMEOUT_HOURS
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_BLOCK_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_MAX_SINGLE_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.container.UploadErrorCode
import com.microsoft.azure.kusto.ingest.v2.container.UploadResult
import com.microsoft.azure.kusto.ingest.v2.container.UploadResults
import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.time.Clock
import java.time.Duration
import java.time.Instant

/** Represents an abstract base class for uploaders to storage containers. */
abstract class ContainerUploaderBase(
    private val retryPolicy: IngestRetryPolicy,
    private val maxConcurrency: Int,
    private val maxDataSize: Long,
    private val configurationCache: ConfigurationCache,
    private val uploadMethod: UploadMethod,
) : IUploader {

    protected val logger: Logger =
        LoggerFactory.getLogger(ContainerUploaderBase::class.java)

    private val effectiveMaxConcurrency: Int =
        minOf(maxConcurrency, Runtime.getRuntime().availableProcessors())

    override var ignoreSizeLimit: Boolean = false

    override fun close() {
        // Default implementation - can be overridden
    }

    override suspend fun uploadAsync(local: LocalSource): BlobSource {
        // Get the stream and validate it
        val stream = local.data()
        val name = local.generateBlobName()

        val errorCode = checkStreamForErrors(stream)
        if (errorCode != null) {
            logger.error(
                "Stream validation failed for {}: {}",
                name,
                errorCode.description,
            )
            throw IngestException(errorCode.description, isPermanent = true)
        }

        // Check size limit if not ignored
        if (!ignoreSizeLimit && stream.available() > 0) {
            val availableSize = stream.available().toLong()
            if (availableSize > maxDataSize) {
                logger.error(
                    "Stream size {} exceeds max allowed size {} for: {}",
                    availableSize,
                    maxDataSize,
                    name,
                )
                throw IngestException(
                    "Upload source exceeds maximum allowed size: $availableSize > $maxDataSize",
                    isPermanent = true,
                )
            }
        }

        // Get containers from configuration
        val containers = selectContainers(configurationCache, uploadMethod)

        if (containers.isEmpty()) {
            logger.error("No containers available for upload")
            throw IngestException(
                "No upload containers available",
                isPermanent = true,
            )
        }

        // Upload with retry policy and container cycling
        return uploadWithRetries(
            local = local,
            name = name,
            stream = stream,
            containers = containers,
        )
    }

    /**
     * Uploads a stream with retry logic and container cycling. Randomly selects
     * a starting container and cycles through containers on each retry. For
     * example, with 2 containers and 3 retries: 1->2->1 or 2->1->2
     */
    private suspend fun uploadWithRetries(
        local: LocalSource,
        name: String,
        stream: InputStream,
        containers: List<ContainerInfo>,
    ): BlobSource {
        // Select random starting container index
        var containerIndex = (0 until containers.size).random()

        logger.debug(
            "Starting upload with {} containers, random start index: {}",
            containers.size,
            containerIndex,
        )

        var retryNumber = 0u
        var lastException: Exception?

        while (true) {
            try {
                val container = containers[containerIndex]

                logger.debug(
                    "Upload attempt {} to container index {} ({}): {}",
                    retryNumber + 1u,
                    containerIndex,
                    container.path?.split("?")?.first() ?: "unknown",
                    name,
                )

                // Perform the actual blob upload
                val blobUrl =
                    uploadToContainer(
                        name = name,
                        stream = stream,
                        container = container,
                        maxConcurrency = effectiveMaxConcurrency,
                    )

                logger.info(
                    "Successfully uploaded {} to container index {} on attempt {}",
                    name,
                    containerIndex,
                    retryNumber + 1u,
                )

                // Return BlobSource with the uploaded blob path
                return BlobSource(
                    blobPath = blobUrl,
                    format = local.format,
                    compressionType = local.compressionType,
                    sourceId = local.sourceId,
                )
                    .apply { blobExactSize = local.size() }
            } catch (e: Exception) {
                lastException = e

                logger.warn(
                    "Upload attempt {} failed to container index {}: {}",
                    retryNumber + 1u,
                    containerIndex,
                    e.message,
                )

                // Don't retry on permanent errors
                if (e is IngestException && e.isPermanent == true) {
                    logger.error(
                        "Permanent error on attempt {}: {}",
                        retryNumber + 1u,
                        e.message,
                    )
                    throw e
                }

                // Check if we should retry
                retryNumber++
                val retryDecision = retryPolicy.moveNext(retryNumber)

                if (!retryDecision.shouldRetry) {
                    logger.error(
                        "Retry policy exhausted after {} attempts",
                        retryNumber,
                    )
                    throw IngestException(
                        "Upload failed after $retryNumber attempts to ${containers.size} container(s)",
                        isPermanent = false,
                        cause = lastException,
                    )
                }

                // Cycle to next container
                containerIndex = (containerIndex + 1) % containers.size

                logger.info(
                    "Retry attempt {} - cycling to container index {}, waiting {} ms",
                    retryNumber,
                    containerIndex,
                    retryDecision.interval.toMillis(),
                )
                // Wait before retrying
                if (retryDecision.interval.toMillis() > 0) {
                    kotlinx.coroutines.delay(retryDecision.interval.toMillis())
                }
            }
        }
    }

    override suspend fun uploadManyAsync(
        localSources: List<LocalSource>,
    ): UploadResults = coroutineScope {
        logger.info(
            "Starting batch upload of {} sources with max concurrency {}",
            localSources.size,
            maxConcurrency,
        )
        // Process sources in chunks to respect maxConcurrency at file level
        val results =
            localSources.chunked(maxConcurrency).flatMap { chunk ->
                chunk.map { source ->
                    async {
                        val startedAt =
                            Instant.now(Clock.systemUTC())
                        try {
                            val blobSource = uploadAsync(source)
                            val completedAt =
                                Instant.now(Clock.systemUTC())
                            UploadResult.Success(
                                sourceName = source.name,
                                startedAt = startedAt,
                                completedAt = completedAt,
                                blobUrl = blobSource.blobPath,
                                sizeBytes = source.size() ?: -1,
                            )
                        } catch (e: Exception) {
                            val completedAt =
                                Instant.now(Clock.systemUTC())
                            val errorCode =
                                when {
                                    e.message?.contains(
                                        "size",
                                    ) == true ->
                                        UploadErrorCode
                                            .SOURCE_SIZE_LIMIT_EXCEEDED
                                    e.message?.contains(
                                        "readable",
                                    ) == true ->
                                        UploadErrorCode
                                            .SOURCE_NOT_READABLE
                                    e.message?.contains(
                                        "empty",
                                    ) == true ->
                                        UploadErrorCode
                                            .SOURCE_IS_EMPTY
                                    e.message?.contains(
                                        "container",
                                    ) == true ->
                                        UploadErrorCode
                                            .NO_CONTAINERS_AVAILABLE
                                    else ->
                                        UploadErrorCode
                                            .UPLOAD_FAILED
                                }

                            UploadResult.Failure(
                                sourceName = source.name,
                                startedAt = startedAt,
                                completedAt = completedAt,
                                errorCode = errorCode,
                                errorMessage =
                                e.message
                                    ?: "Upload failed",
                                exception = e,
                                isPermanent =
                                e is IngestException &&
                                    e.isPermanent ==
                                    true,
                            )
                        }
                    }
                }
                    .awaitAll()
            }

        val successes = results.filterIsInstance<UploadResult.Success>()
        val failures = results.filterIsInstance<UploadResult.Failure>()

        logger.info(
            "Batch upload completed: {} successes, {} failures out of {} total",
            successes.size,
            failures.size,
            localSources.size,
        )

        UploadResults(successes, failures)
    }

    /** Validates the stream for ingestion. */
    private fun checkStreamForErrors(stream: InputStream?): UploadErrorCode? {
        if (stream == null) {
            return UploadErrorCode.SOURCE_IS_NULL
        }
        val length = estimateStreamLength(stream)
        if (length < 0) {
            return UploadErrorCode.SOURCE_NOT_READABLE
        }
        if (length == 0L) {
            return UploadErrorCode.SOURCE_IS_EMPTY
        }
        if (length > maxDataSize && !ignoreSizeLimit) {
            return UploadErrorCode.SOURCE_SIZE_LIMIT_EXCEEDED
        }
        return null
    }

    private fun estimateStreamLength(stream: InputStream): Long {
        return try {
            stream.available().toLong()
        } catch (_: Exception) {
            -1L
        }
    }

    protected fun uploadToContainer(
        name: String,
        stream: InputStream,
        container: ContainerInfo,
        maxConcurrency: Int,
    ): String {
        val (url, sas) = container.path!!.split("?", limit = 2)

        val blobClient =
            BlobClientBuilder()
                .endpoint(container.path)
                .blobName(name)
                .buildClient()

        logger.debug(
            "Uploading stream to blob url: {} to container {}",
            url,
            name,
        )

        val parallelTransferOptions =
            ParallelTransferOptions()
                .setBlockSizeLong(UPLOAD_BLOCK_SIZE_BYTES)
                .setMaxConcurrency(maxConcurrency)
                .setMaxSingleUploadSizeLong(
                    UPLOAD_MAX_SINGLE_SIZE_BYTES,
                )

        val blobUploadOptions =
            BlobParallelUploadOptions(stream)
                .setParallelTransferOptions(parallelTransferOptions)

        val blobUploadResult =
            blobClient.uploadWithResponse(
                blobUploadOptions,
                Duration.ofHours(BLOB_UPLOAD_TIMEOUT_HOURS),
                Context.NONE,
            )

        return if (
            blobUploadResult.statusCode in 200..299 &&
            blobUploadResult.value != null
        ) {
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
                isPermanent = blobUploadResult.statusCode in 400..<500,
            )
        }
    }

    /**
     * Selects the appropriate containers for upload based on the provided
     * configuration cache and upload method.
     *
     * @param configurationCache The configuration cache to use for selecting
     *   containers.
     * @param uploadMethod The upload method to consider when selecting
     *   containers.
     * @return A list of selected container information.
     */
    abstract suspend fun selectContainers(
        configurationCache: ConfigurationCache,
        uploadMethod: UploadMethod,
    ): List<ContainerInfo>
}
