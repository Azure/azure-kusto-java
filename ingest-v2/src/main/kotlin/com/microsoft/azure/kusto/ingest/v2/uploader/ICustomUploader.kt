// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResults
import kotlinx.coroutines.future.await
import java.io.Closeable
import java.util.concurrent.CompletableFuture

/**
 * Java-compatible interface for creating custom uploaders.
 *
 * This interface uses [CompletableFuture] instead of Kotlin coroutines,
 * allowing Java developers to implement custom upload logic without needing to
 * understand Kotlin suspend functions.
 */
interface ICustomUploader : Closeable {
    /**
     * Indicates whether to ignore the max data size allowed during upload.
     * Default should be false.
     */
    fun getIgnoreSizeLimit(): Boolean

    /** Sets whether to ignore the max data size limit. */
    fun setIgnoreSizeLimit(value: Boolean)

    /**
     * Uploads the specified local source asynchronously.
     *
     * @param local The local source to upload.
     * @return A CompletableFuture that completes with the uploaded blob source.
     */
    fun uploadAsync(local: LocalSource): CompletableFuture<BlobSource>

    /**
     * Uploads the specified local sources asynchronously.
     *
     * @param localSources List of the local sources to upload.
     * @return A CompletableFuture that completes with the upload results.
     */
    fun uploadManyAsync(
        localSources: List<LocalSource>,
    ): CompletableFuture<UploadResults>
}

/**
 * Extension function to convert [ICustomUploader] to [IUploader].
 *
 * Kotlin users can use this as: `myCustomUploader.asUploader()`
 */
fun ICustomUploader.asUploader(): IUploader = CustomUploaderAdapter(this)

/**
 * Static helper methods for [ICustomUploader].
 *
 * Provides Java-friendly static methods to work with custom uploaders.
 */
object CustomUploaderHelper {
    /**
     * Wraps an [ICustomUploader] with an adapter to create an [IUploader].
     *
     * This is the Java-friendly way to convert a custom uploader:
     */
    @JvmStatic
    fun asUploader(customUploader: ICustomUploader): IUploader =
        CustomUploaderAdapter(customUploader)
}

/**
 * Adapter that wraps an [ICustomUploader] to implement the [IUploader]
 * interface.
 *
 * This allows Java-implemented uploaders to be used anywhere an [IUploader] is
 * expected, such as with QueuedIngestClient or ManagedStreamingIngestClient.
 */
class CustomUploaderAdapter(private val customUploader: ICustomUploader) :
    IUploader {
    override var ignoreSizeLimit: Boolean
        get() = customUploader.getIgnoreSizeLimit()
        set(value) {
            customUploader.setIgnoreSizeLimit(value)
        }

    override suspend fun uploadAsync(local: LocalSource): BlobSource {
        return customUploader.uploadAsync(local).await()
    }

    override suspend fun uploadManyAsync(
        localSources: List<LocalSource>,
    ): UploadResults {
        return customUploader.uploadManyAsync(localSources).await()
    }

    override fun close() {
        customUploader.close()
    }
}
