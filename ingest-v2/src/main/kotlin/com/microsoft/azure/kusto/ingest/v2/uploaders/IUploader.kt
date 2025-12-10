// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploaders

import com.microsoft.azure.kusto.ingest.v2.container.UploadResult
import com.microsoft.azure.kusto.ingest.v2.container.UploadResults
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import java.io.Closeable

/** Interface for uploading data sources to blob storage. */
interface IUploader : Closeable {
    /**
     * Indicates whether to ignore the max data size allowed during the upload
     * operation. Default is false.
     */
    var ignoreSizeLimit: Boolean

    /**
     * Uploads the specified local source.
     *
     * @param local The local source to upload.
     * @return The uploaded blob source.
     */
    suspend fun uploadAsync(local: LocalSource): BlobSource

    /**
     * Uploads the specified local sources.
     *
     * @param localSources List of the local sources to upload.
     * @return The uploaded results - successes (as [UploadResult.Success]) and
     *   failures (as [UploadResult.Failure]).
     */
    suspend fun uploadManyAsync(localSources: List<LocalSource>): UploadResults
}
