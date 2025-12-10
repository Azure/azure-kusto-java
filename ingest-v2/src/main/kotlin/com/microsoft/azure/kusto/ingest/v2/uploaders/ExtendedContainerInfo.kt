// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploaders

import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo

data class ExtendedContainerInfo(
    val containerInfo: ContainerInfo,
    val uploadMethod: UploadMethod,
)
