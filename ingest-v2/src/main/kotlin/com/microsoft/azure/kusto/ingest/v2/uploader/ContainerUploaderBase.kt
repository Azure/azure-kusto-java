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
import com.microsoft.azure.kusto.ingest.v2.STREAM_COMPRESSION_BUFFER_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.STREAM_PIPE_BUFFER_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_BLOCK_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_MAX_SINGLE_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import com.microsoft.azure.kusto.ingest.v2.uploader.compression.CompressionException
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadErrorCode
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResult
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResults
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.future
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
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

    /**
     * Atomic counter for round-robin container selection. Increments on each
     * upload to distribute load evenly across containers.
     */
    private val containerIndexCounter = AtomicInteger(0)

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
        val preparedStream =
            if (local.shouldCompress) {
                logger.debug(
                    "Auto-compressing stream for {} (format: {}, original compression: {})",
                    name,
                    local.format,
                    local.compressionType,
                )
                val compressResult = compressStreamWithPipe(originalStream)
                logger.debug(
                    "Compression started for {} using streaming approach (original={} bytes)",
                    name,
                    availableSize,
                )
                PreparedUploadStream(
                    stream = compressResult.stream,
                    compressionType = CompressionType.GZIP,
                    compressionJob = compressResult.compressionJob,
                )
            } else {
                PreparedUploadStream(
                    stream = originalStream,
                    compressionType = local.compressionType,
                    compressionJob = null,
                )
            }

        // Upload with retry policy and container cycling
        return try {
            uploadWithRetries(
                local = local,
                name = name,
                stream = preparedStream.stream,
                containers = containers,
                effectiveCompressionType = preparedStream.compressionType,
            )
                .also {
                    // Ensure compression job completes successfully
                    preparedStream.compressionJob?.await()
                    logger.debug(
                        "Compression job completed successfully for {}",
                        name,
                    )
                }
        } catch (e: Exception) {
            // Cancel compression job if upload fails
            preparedStream.compressionJob?.cancel()
            throw e
        }
    }

    /**
     * Compresses the input stream using GZIP compression with streaming
     * approach. Uses piped streams to avoid loading entire file into memory.
     *
     * This creates a background coroutine that reads from the input stream,
     * compresses the data, and writes to a pipe. The returned InputStream reads
     * from the other end of the pipe, allowing the uploader to stream
     * compressed bytes directly into storage.
     */
    private suspend fun compressStreamWithPipe(
        inputStream: InputStream,
    ): CompressedStreamResult =
        withContext(Dispatchers.IO) {
            try {
                // Create piped streams with 1MB buffer to handle backpressure
                val pipeSize = STREAM_PIPE_BUFFER_SIZE_BYTES
                val pipedInputStream = PipedInputStream(pipeSize)
                val pipedOutputStream = PipedOutputStream(pipedInputStream)

                logger.debug(
                    "Starting streaming GZIP compression with pipe buffer size: {} bytes",
                    pipeSize,
                )

                // Start compression in background coroutine
                val compressionJob =
                    async(Dispatchers.IO) {
                        try {
                            GZIPOutputStream(
                                pipedOutputStream,
                                STREAM_COMPRESSION_BUFFER_SIZE_BYTES,
                            )
                                .use { gzipStream ->
                                    inputStream.use { input ->
                                        input.copyTo(
                                            gzipStream,
                                            bufferSize =
                                            STREAM_COMPRESSION_BUFFER_SIZE_BYTES,
                                        )
                                    }
                                }
                        } catch (e: IOException) {
                            logger.error(
                                "Streaming GZIP compression failed: {}",
                                e.message,
                            )
                            // Close output pipe to signal error to reader
                            try {
                                pipedOutputStream.close()
                            } catch (_: Exception) {
                                // Ignore close errors during cleanup
                            }
                            throw CompressionException(
                                "Failed to compress stream using streaming GZIP",
                                e,
                            )
                        } catch (e: OutOfMemoryError) {
                            logger.error(
                                "Streaming GZIP compression failed due to memory constraints: {}",
                                e.message,
                            )
                            try {
                                pipedOutputStream.close()
                            } catch (_: Exception) {
                                // Ignore close errors during cleanup
                            }
                            throw CompressionException(
                                "Insufficient memory for streaming compression",
                                e,
                            )
                        }
                    }

                CompressedStreamResult(pipedInputStream, compressionJob)
            } catch (e: IOException) {
                logger.error(
                    "Failed to setup compression pipes: {}",
                    e.message,
                )
                throw CompressionException(
                    "Failed to initialize streaming compression",
                    e,
                )
            }
        }

    /** Helper class to hold compressed stream and its completion job */
    private data class CompressedStreamResult(
        val stream: InputStream,
        val compressionJob: kotlinx.coroutines.Deferred<Long>,
    )

    /**
     * Helper class to hold prepared upload stream with its compression type
     * and optional compression job.
     */
    private data class PreparedUploadStream(
        val stream: InputStream,
        val compressionType: CompressionType,
        val compressionJob: kotlinx.coroutines.Deferred<Long>?,
    )

    /**
     * Uploads a stream with retry logic and container cycling. Uses an
     * incrementing counter (mod container count) for round-robin container
     * selection, ensuring even load distribution across containers on each
     * retry. For example, with 2 containers and 3 retries: 0->1->0 or 1->0->1
     */
    private suspend fun uploadWithRetries(
        local: LocalSource,
        name: String,
        stream: InputStream,
        containers: List<ExtendedContainerInfo>,
        effectiveCompressionType: CompressionType = local.compressionType,
    ): BlobSource {
        // Select container using incrementing counter for round-robin distribution
        // Note: Math.floorMod handles negative values correctly if overflow occurs
        var containerIndex =
            Math.floorMod(containerIndexCounter.getAndIncrement(), containers.size)

        logger.debug(
            "Starting upload with {} containers, round-robin index: {}",
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
                    sourceId = local.sourceId,
                    compressionType = effectiveCompressionType,
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
        // TODO check and validate failure scenarios
        // Use semaphore for true streaming parallelism
        // This allows up to effectiveMaxConcurrency concurrent uploads, starting new ones as soon as slots
        // are available
        val semaphore = Semaphore(effectiveMaxConcurrency)

        // Launch all uploads concurrently, but semaphore limits actual concurrent execution
        val results =
            localSources
                .map { source ->
                    async {
                        semaphore.withPermit {
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
                }
                .awaitAll()

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
     * Uploads the specified local source asynchronously. This is the
     * Java-compatible version that returns a CompletableFuture.
     *
     * @param local The local source to upload.
     * @return A CompletableFuture that will complete with the uploaded blob
     *   source.
     */
    @JvmName("uploadAsync")
    fun uploadAsyncJava(local: LocalSource): CompletableFuture<BlobSource> =
        CoroutineScope(Dispatchers.IO).future { uploadAsync(local) }

    /**
     * Uploads the specified local sources asynchronously. This is the
     * Java-compatible version that returns a CompletableFuture.
     *
     * @param localSources List of the local sources to upload.
     * @return A CompletableFuture that will complete with the upload results.
     */
    @JvmName("uploadManyAsync")
    fun uploadManyAsyncJava(
        localSources: List<LocalSource>,
    ): CompletableFuture<UploadResults> =
        CoroutineScope(Dispatchers.IO).future {
            uploadManyAsync(localSources)
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
