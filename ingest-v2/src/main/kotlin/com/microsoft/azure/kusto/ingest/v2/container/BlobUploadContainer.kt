// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.container

import com.azure.storage.blob.BlobClientBuilder
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import org.slf4j.LoggerFactory
import java.io.InputStream

class BlobUploadContainer(val configurationCache: ConfigurationCache) :
    UploadContainerBase {
    private val logger =
        LoggerFactory.getLogger(BlobUploadContainer::class.java)

    // choose a random container from the configResponse.containerSettings.containers
    override suspend fun uploadAsync(
        name: String,
        stream: InputStream,
    ): String {
        val (url, sas, targetPath) = getBlobTargetInfo(name)
        val blobClient =
            BlobClientBuilder()
                .endpoint(targetPath.path)
                .blobName(name)
                .buildClient()
        logger.debug("Uploading stream to blob url: {} to container {}", url, name)
        // TODO Check on parallel uploads, retries to be implemented. Explore upload from File API
        // TODO What is the size of the stream, should we use uploadFromFile API?
        blobClient.upload(stream, true)
        return "$url/$name?$sas"
    }


    suspend fun uploadFromFileAsync(
        name: String,
        filePath: String,
    ): String {
        val (url, sas, targetPath) = getBlobTargetInfo(name)
        val blobClient =
            BlobClientBuilder()
                .endpoint(targetPath.path)
                .blobName(name)
                .buildClient()
        logger.debug("Uploading file {} to blob url: {} to container {}", filePath, url, name)
        blobClient.uploadFromFile(filePath, true)
        return "$url/$name?$sas"
    }

    private suspend fun getBlobTargetInfo(name: String): Triple<String, String, com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo> {
        val configResponse = configurationCache.getConfiguration()
        // Placeholder for actual upload logic
        // In a real implementation, this would upload the stream to the blob storage
        // and return the URI of the uploaded blob.
        // check if the configResponse has containerSettings
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
        // check if containers is null or empty , if so use lakeFolders and choose one randomly
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
        return Triple(url, sas, targetPath)
    }
}
