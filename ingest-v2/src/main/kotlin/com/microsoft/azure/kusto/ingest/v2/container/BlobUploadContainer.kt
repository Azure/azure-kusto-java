// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.container

import com.azure.core.util.Context
import com.azure.storage.blob.BlobClientBuilder
import com.azure.storage.blob.models.ParallelTransferOptions
import com.azure.storage.blob.options.BlobUploadFromFileOptions
import com.azure.storage.common.policy.RequestRetryOptions
import com.azure.storage.common.policy.RetryPolicyType
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.time.Duration

private const val BLOCK_SIZE_BYTES: Long = 4 * 1024 * 1024 // 4 MB block size
private const val MAX_CONCURRENCY: Int = 8 // 8 concurrent requests
private const val MAX_SINGLE_UPLOAD_SIZE_BYTES: Long =
    256 * 1024 * 1024 // 256 MB max single upload

private val DEFAULT_RETRY_OPTIONS =
    RequestRetryOptions(
        RetryPolicyType.EXPONENTIAL,
        // 3 retries
        3,
        // Try timeout in seconds to 1 min
        60,
        // Retry delay in ms (default)
        100,
        // Max retry delay in ms (default)
        300,
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
        blobClient.upload(stream, true)
        return "${targetInfo.url}/$name?${targetInfo.sas}"
    }

    suspend fun uploadFromFileAsync(name: String, filePath: String): String {
        val targetInfo = getBlobTargetInfo()
        val blobClient =
            BlobClientBuilder()
                .endpoint(targetInfo.containerInfo.path)
                .blobName(name)
                .retryOptions(DEFAULT_RETRY_OPTIONS)
                .buildClient()
        logger.debug(
            "Uploading file {} to blob url: {} to container {}",
            filePath,
            targetInfo.url,
            name,
        )
        val parallelTransferOptions =
            ParallelTransferOptions()
                .setBlockSizeLong(BLOCK_SIZE_BYTES)
                .setMaxConcurrency(MAX_CONCURRENCY)
                .setMaxSingleUploadSizeLong(
                    MAX_SINGLE_UPLOAD_SIZE_BYTES,
                )

        return try {
            val response =
                blobClient.uploadFromFileWithResponse(
                    BlobUploadFromFileOptions(filePath)
                        .setParallelTransferOptions(
                            parallelTransferOptions,
                        ),
                    Duration.ofHours(1),
                    Context.NONE,
                )
            if (response.statusCode in 200..299 && response.value != null) {
                "${targetInfo.url}/$name?${targetInfo.sas}"
            } else {
                throw IngestException(
                    "Upload failed with status: ${response.statusCode}",
                )
            }
        } catch (e: Exception) {
            throw IngestException("Upload failed", e)
        }
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
