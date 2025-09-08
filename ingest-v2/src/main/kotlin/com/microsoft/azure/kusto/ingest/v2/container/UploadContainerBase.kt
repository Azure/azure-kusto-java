/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2.container

import java.io.InputStream

interface UploadContainerBase : ContainerBase {
    suspend fun uploadAsync(name: String, stream: InputStream): String
}
