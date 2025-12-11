// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResults
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
     * @return The uploaded results - successes (as
     *   [com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResult.Success])
     *   and failures (as
     *   [com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResult.Failure]).
     */
    suspend fun uploadManyAsync(localSources: List<LocalSource>): UploadResults
}
