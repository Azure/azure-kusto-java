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
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_BLOCK_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_MAX_CONCURRENCY
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_MAX_SINGLE_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_RETRY_DELAY_MS
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_RETRY_MAX_DELAY_MS
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_RETRY_MAX_TRIES
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_RETRY_TIMEOUT_SECONDS
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.time.Duration

private val DEFAULT_RETRY_OPTIONS =
    RequestRetryOptions(
        RetryPolicyType.EXPONENTIAL,
        // 3 retries
        UPLOAD_RETRY_MAX_TRIES,
        // Try timeout in seconds to 1 min
        UPLOAD_RETRY_TIMEOUT_SECONDS,
        // Retry delay in ms (default)
        UPLOAD_RETRY_DELAY_MS,
        // Max retry delay in ms (default)
        UPLOAD_RETRY_MAX_DELAY_MS,
        // Secondary host (default)
        null,
    )

class BlobUploadContainer(val configurationCache: ConfigurationCache) :
    UploadContainerBase {
    private val logger =
        LoggerFactory.getLogger(BlobUploadContainer::class.java)

    // choose a random container from the configResponse.containerSettings.containers
    override suspend fun uploadAsync(
        name: String,
        stream: InputStream,
    ): String {
        val targetInfo = getBlobTargetInfo()
        val blobClient =
            BlobClientBuilder()
                .endpoint(targetInfo.containerInfo.path)
                .blobName(name)
                .buildClient()
        logger.debug(
            "Uploading stream to blob url: {} to container {}",
            targetInfo.url,
            name,
        )
        // TODO Check on parallel uploads, retries to be implemented. Explore upload from File API
        // TODO What is the size of the stream, should we use uploadFromFile API?
        val parallelTransferOptions =
            ParallelTransferOptions()
                .setBlockSizeLong(UPLOAD_BLOCK_SIZE_BYTES)
                .setMaxConcurrency(UPLOAD_MAX_CONCURRENCY)
                .setMaxSingleUploadSizeLong(
                    UPLOAD_MAX_SINGLE_SIZE_BYTES,
                )
        val blobUploadOptions =
            BlobParallelUploadOptions(stream)
                .setParallelTransferOptions(parallelTransferOptions)
        val blobUploadResult =
            blobClient.uploadWithResponse(
                blobUploadOptions,
                Duration.ofHours(1),
                Context.NONE,
            )
        if (
            blobUploadResult.statusCode in 200..299 &&
            blobUploadResult.value != null
        ) {
            val blockBlobItem: BlockBlobItem = blobUploadResult.value
            logger.debug(
                "Upload succeeded to blob url: {} with eTag: {}",
                targetInfo.url,
                blockBlobItem.eTag,
            )
            "${targetInfo.url}/$name?${targetInfo.sas}"
        } else {
            throw IngestException(
                "Upload failed with status: ${blobUploadResult.statusCode}",
            )
        }
        return "${targetInfo.url}/$name?${targetInfo.sas}"
    }

    private suspend fun getBlobTargetInfo(): BlobTargetInfo {
        val configResponse = configurationCache.getConfiguration()
        val noUploadLocation =
            configResponse.containerSettings == null ||
                (
                    configResponse.containerSettings.containers
                        ?.isEmpty() == true &&
                        configResponse.containerSettings.lakeFolders
                            ?.isEmpty() == true
                    )
        if (noUploadLocation) {
            throw IngestException(
                "No container settings available in the configuration response",
            )
        }
        val targetPath =
            if (
                configResponse.containerSettings.containers
                    .isNullOrEmpty()
            ) {
                configResponse.containerSettings.lakeFolders!!.random()
            } else {
                configResponse.containerSettings.containers.random()
            }
        val (url, sas) = targetPath.path!!.split("?")
        return BlobTargetInfo(url, sas, targetPath)
    }
}

private data class BlobTargetInfo(
    val url: String,
    val sas: String,
    val containerInfo: ContainerInfo,
)
