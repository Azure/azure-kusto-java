// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.azure.core.credential.TokenCredential
import com.azure.core.util.Context
import com.azure.storage.blob.BlobClientBuilder
import com.azure.storage.blob.models.BlockBlobItem
import com.azure.storage.blob.options.BlobParallelUploadOptions
import com.azure.storage.common.ParallelTransferOptions
import com.azure.storage.file.datalake.DataLakeFileClient
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder
import com.azure.storage.file.datalake.options.FileParallelUploadOptions
import com.microsoft.azure.kusto.ingest.v2.BLOB_UPLOAD_TIMEOUT_HOURS
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_BLOCK_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_MAX_SINGLE_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadErrorCode
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResult
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResults
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.zip.GZIPOutputStream

/** Represents an abstract base class for uploaders to storage containers. */
abstract class ContainerUploaderBase(
    private val retryPolicy: IngestRetryPolicy,
    private val maxConcurrency: Int,
    private val maxDataSize: Long,
    private val configurationCache: ConfigurationCache,
    private val uploadMethod: UploadMethod,
    private val tokenCredential: TokenCredential?,
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
        val originalStream = local.data()
        val name = local.generateBlobName()

        val errorCode = checkStreamForErrors(originalStream)
        if (errorCode != null) {
            logger.error(
                "Stream validation failed for {}: {}",
                name,
                errorCode.description,
            )
            throw IngestException(errorCode.description, isPermanent = true)
        }

        // Check size limit if not ignored (check original size before compression)
        val availableSize =
            withContext(Dispatchers.IO) { originalStream.available() }
                .toLong()
        if (!ignoreSizeLimit && availableSize > 0) {
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

        // Compress stream if needed (for non-binary, non-compressed formats)
        val (uploadStream, effectiveCompressionType) =
            if (local.shouldCompress) {
                logger.debug(
                    "Auto-compressing stream for {} (format: {}, original compression: {})",
                    name,
                    local.format,
                    local.compressionType,
                )
                val compressedStream = compressStream(originalStream)
                logger.debug(
                    "Compression complete for {}: original={} bytes, compressed={} bytes",
                    name,
                    availableSize,
                    compressedStream.available(),
                )
                Pair(compressedStream, CompressionType.GZIP)
            } else {
                Pair(originalStream, local.compressionType)
            }

        // Upload with retry policy and container cycling
        return uploadWithRetries(
            local = local,
            name = name,
            stream = uploadStream,
            containers = containers,
            effectiveCompressionType = effectiveCompressionType,
        )
    }

    /**
     * Compresses the input stream using GZIP compression. Reads all data into
     * memory, compresses it, and returns a new ByteArrayInputStream.
     */
    private suspend fun compressStream(
        inputStream: InputStream,
    ): ByteArrayInputStream =
        withContext(Dispatchers.IO) {
            val byteArrayOutputStream = ByteArrayOutputStream()
            GZIPOutputStream(byteArrayOutputStream).use { gzipStream ->
                inputStream.copyTo(gzipStream)
            }
            ByteArrayInputStream(byteArrayOutputStream.toByteArray())
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
        containers: List<ExtendedContainerInfo>,
        effectiveCompressionType: CompressionType = local.compressionType,
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
                    container.containerInfo.path?.split("?")?.first()
                        ?: "unknown",
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
                // Use effective compression type (GZIP if auto-compressed)
                return BlobSource(
                    blobPath = blobUrl,
                    format = local.format,
                    compressionType = effectiveCompressionType,
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
                    delay(retryDecision.interval.toMillis())
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
        container: ExtendedContainerInfo,
        maxConcurrency: Int,
    ): String {
        val containerPath = container.containerInfo.path!!
        // Parse URL and SAS token (if present)
        // Storage containers have SAS tokens: "https://...?sp=..."
        // Lake containers don't have SAS tokens:
        // "https://msit-onelake.dfs.fabric.microsoft.com/..."
        val pathParts = containerPath.split("?", limit = 2)
        val url = pathParts[0]
        val sas = if (pathParts.size > 1) pathParts[1] else null

        return if (container.uploadMethod == UploadMethod.STORAGE) {
            // Use Blob API for STORAGE upload method
            uploadUsingBlobApi(
                name,
                stream,
                containerPath,
                url,
                sas,
                maxConcurrency,
            )
        } else {
            // Use Data Lake API for LAKE upload method
            uploadUsingDataLakeApi(name, stream, url, sas, maxConcurrency)
        }
    }

    private fun uploadUsingBlobApi(
        name: String,
        stream: InputStream,
        containerPath: String,
        url: String,
        sas: String?,
        maxConcurrency: Int,
    ): String {
        logger.info(
            "Upload {} using STORAGE upload method for container url {}",
            name,
            url,
        )

        val blobClient =
            BlobClientBuilder()
                .endpoint(containerPath)
                .blobName(name)
                .buildClient()

        val parallelTransferOptions =
            com.azure.storage.blob.models
                .ParallelTransferOptions()
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
            // Return the blob URL with SAS token if available
            if (sas != null) {
                "$url/$name?$sas"
            } else {
                "$url/$name"
            }
        } else {
            throw IngestException(
                "Upload failed with status: ${blobUploadResult.statusCode}",
                isPermanent = blobUploadResult.statusCode in 400..<500,
            )
        }
    }

    private fun uploadUsingDataLakeApi(
        name: String,
        stream: InputStream,
        url: String,
        sas: String?,
        maxConcurrency: Int,
    ): String {
        logger.info(
            "Upload {} using LAKE upload method (Data Lake API) for container url {}",
            name,
            url,
        )

        // Parse the URL to extract file system and path
        // OneLake URL format:
        // https://msit-onelake.dfs.fabric.microsoft.com/{workspace-id}/{lakehouse-id}/Files/Ingestions/
        // In OneLake/Fabric, the workspace-id is treated as the "container" (file system in ADLS
        // Gen2 terms)
        // and {lakehouse-id}/Files/... is the path within that container
        val uri = java.net.URI(url)
        val pathSegments = uri.path.trimStart('/').split('/')

        val serviceEndpoint = "${uri.scheme}://${uri.host}"
        // First segment is the workspace-id (container/filesystem)
        val fileSystemName =
            if (pathSegments.isNotEmpty()) pathSegments[0] else ""
        // Remaining segments form the directory path: {lakehouse-id}/Files/Ingestions/...
        val directoryPath =
            if (pathSegments.size > 1) {
                pathSegments
                    .subList(1, pathSegments.size)
                    .filter { it.isNotEmpty() }
                    .joinToString("/")
            } else {
                ""
            }

        // Build the Data Lake file client
        val fileClient: DataLakeFileClient =
            if (tokenCredential != null) {
                logger.debug(
                    "Using TokenCredential for Data Lake authentication",
                )
                val serviceClient =
                    DataLakeServiceClientBuilder()
                        .endpoint(serviceEndpoint)
                        .credential(tokenCredential)
                        .buildClient()

                val fileSystemClient =
                    serviceClient.getFileSystemClient(fileSystemName)
                if (directoryPath.isNotEmpty()) {
                    fileSystemClient
                        .getDirectoryClient(directoryPath)
                        .getFileClient(name)
                } else {
                    fileSystemClient.getFileClient(name)
                }
            } else if (sas != null) {
                logger.debug("Using SAS token for Data Lake authentication")
                val serviceClient =
                    DataLakeServiceClientBuilder()
                        .endpoint("$serviceEndpoint?$sas")
                        .buildClient()

                val fileSystemClient =
                    serviceClient.getFileSystemClient(fileSystemName)
                if (directoryPath.isNotEmpty()) {
                    fileSystemClient
                        .getDirectoryClient(directoryPath)
                        .getFileClient(name)
                } else {
                    fileSystemClient.getFileClient(name)
                }
            } else {
                logger.debug("Using anonymous access for Data Lake")
                val serviceClient =
                    DataLakeServiceClientBuilder()
                        .endpoint(serviceEndpoint)
                        .buildClient()

                val fileSystemClient =
                    serviceClient.getFileSystemClient(fileSystemName)
                if (directoryPath.isNotEmpty()) {
                    fileSystemClient
                        .getDirectoryClient(directoryPath)
                        .getFileClient(name)
                } else {
                    fileSystemClient.getFileClient(name)
                }
            }

        val parallelTransferOptions =
            ParallelTransferOptions()
                .setBlockSizeLong(UPLOAD_BLOCK_SIZE_BYTES)
                .setMaxConcurrency(maxConcurrency)
                .setMaxSingleUploadSizeLong(
                    UPLOAD_MAX_SINGLE_SIZE_BYTES,
                )

        val uploadResponse =
            fileClient.uploadWithResponse(
                FileParallelUploadOptions(stream)
                    .setParallelTransferOptions(
                        parallelTransferOptions,
                    ),
                Duration.ofHours(BLOB_UPLOAD_TIMEOUT_HOURS),
                Context.NONE,
            )

        return if (uploadResponse.statusCode in 200..299) {
            logger.debug(
                "Upload succeeded to Data Lake file: {} with eTag: {}",
                name,
                uploadResponse.value?.eTag,
            )
            // Return the file URL with SAS token if available
            if (sas != null) {
                "$url/$name?$sas"
            } else {
                "$url/$name"
            }
        } else {
            throw IngestException(
                "Data Lake upload failed with status: ${uploadResponse.statusCode}",
                isPermanent = uploadResponse.statusCode in 400..<500,
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
    ): List<ExtendedContainerInfo>
}
