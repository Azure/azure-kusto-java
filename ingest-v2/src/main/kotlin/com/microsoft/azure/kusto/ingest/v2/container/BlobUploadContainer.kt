// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.container

import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import org.jetbrains.annotations.NotNull

class BlobUploadContainer(val configResponse: @NotNull ConfigurationResponse) :
    UploadContainerBase {

    // choose a random container from the configResponse.containerSettings.containers

    override suspend fun uploadAsync(
        name: String,
        stream: java.io.InputStream,
    ): String {
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
        return "${targetPath.path}/$name"
    }
}
