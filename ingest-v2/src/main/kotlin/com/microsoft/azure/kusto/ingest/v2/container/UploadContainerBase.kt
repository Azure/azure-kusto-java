// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.container

import java.io.InputStream

interface UploadContainerBase {
    suspend fun uploadAsync(name: String, stream: InputStream): String
}
