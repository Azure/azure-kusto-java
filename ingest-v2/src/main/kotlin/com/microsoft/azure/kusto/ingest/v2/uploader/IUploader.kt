// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResults
import java.io.Closeable
import java.util.concurrent.CompletableFuture

/**
 * Interface for uploading data sources to blob storage.
 *
 * **Java Interoperability:**
 *
 * This interface provides two sets of methods:
 * - **Kotlin suspend functions** (`uploadAsync`, `uploadManyAsync`) — for
 *   Kotlin coroutine-based callers.
 * - **Java-friendly methods** (`uploadAsyncJava`, `uploadManyAsyncJava`) —
 *   returning [CompletableFuture] for Java callers who want to program to this
 *   interface.
 *
 * **For Java developers** who want to implement their own uploader, there are
 * two options:
 * 1. **Recommended:** Implement [ICustomUploader] (pure Java, uses
 *    [CompletableFuture]), then wrap it using [CustomUploaderHelper.asUploader]
 *    to get an [IUploader].
 * 2. **Direct implementation:** Implement [IUploader] directly if you are
 *    comfortable with Kotlin suspend functions in your Java project.
 *
 * @see ICustomUploader
 * @see CustomUploaderHelper
 */
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

    // =========================================================================
    // Java-friendly methods returning CompletableFuture
    // These have the "Java" suffix to avoid JVM signature conflicts with
    // suspend functions.
    // =========================================================================

    /**
     * Uploads the specified local source. This is the Java-friendly version
     * that returns a [CompletableFuture].
     *
     * @param local The local source to upload.
     * @return A [CompletableFuture] that completes with the uploaded blob
     *   source.
     */
    fun uploadAsyncJava(local: LocalSource): CompletableFuture<BlobSource>

    /**
     * Uploads the specified local sources. This is the Java-friendly version
     * that returns a [CompletableFuture].
     *
     * @param localSources List of the local sources to upload.
     * @return A [CompletableFuture] that completes with the upload results -
     *   successes (as
     *   [com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResult.Success])
     *   and failures (as
     *   [com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResult.Failure]).
     */
    fun uploadManyAsyncJava(
        localSources: List<LocalSource>,
    ): CompletableFuture<UploadResults>
}
