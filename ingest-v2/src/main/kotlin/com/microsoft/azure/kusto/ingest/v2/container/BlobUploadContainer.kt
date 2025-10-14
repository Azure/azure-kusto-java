// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.container

import com.azure.storage.blob.BlobClientBuilder
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException

class BlobUploadContainer(val configurationCache: ConfigurationCache) :
    UploadContainerBase {
    // choose a random container from the configResponse.containerSettings.containers
    override suspend fun uploadAsync(
        name: String,
        stream: java.io.InputStream,
    ): String {
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
        val blobTargetUrl = "${targetPath.path}/$name"
        // Ensure the endpoint is a valid full URL for Azure BlobClientBuilder
        val blobClient =
            BlobClientBuilder().endpoint(blobTargetUrl).buildClient()
        blobClient.upload(stream, true)
        return blobTargetUrl
    }
}
