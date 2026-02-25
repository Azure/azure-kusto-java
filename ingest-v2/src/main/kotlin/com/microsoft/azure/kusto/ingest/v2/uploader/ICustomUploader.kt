// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResults
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import java.io.Closeable
import java.util.concurrent.CompletableFuture

/**
 * Java-compatible interface for creating custom uploaders.
 *
 * This interface uses [CompletableFuture] instead of Kotlin coroutines,
 * allowing Java developers to implement custom upload logic without needing to
 * understand Kotlin suspend functions.
 *
 * **Usage from Java:**
 * 1. Implement this interface with your custom upload logic.
 * 2. Wrap it using [CustomUploaderHelper.asUploader] to get an [IUploader].
 * 3. Pass the [IUploader] to
 *    [com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder.withUploader].
 *
 * The resulting [IUploader] exposes both Kotlin suspend functions and
 * Java-friendly [CompletableFuture] methods (`uploadAsyncJava`,
 * `uploadManyAsyncJava`), so it can be used from either language.
 *
 * **Example (Java):**
 *
 * ```java
 * ICustomUploader custom = new MyCustomUploader(...);
 * IUploader uploader = CustomUploaderHelper.asUploader(custom);
 *
 * // Use directly:
 * CompletableFuture<BlobSource> result = uploader.uploadAsyncJava(localSource);
 *
 * // Or pass to a client builder:
 * QueuedIngestClient client = QueuedIngestClientBuilder.create(dmUrl)
 *     .withAuthentication(credential)
 *     .withUploader(uploader, true)
 *     .build();
 * ```
 *
 * @see IUploader
 * @see CustomUploaderHelper
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
 * Static helper methods for [ICustomUploader].
 *
 * Provides Java-friendly static methods to work with custom uploaders.
 *
 * **Example (Java):**
 *
 * ```java
 * ICustomUploader custom = new MyCustomUploader(...);
 * IUploader uploader = CustomUploaderHelper.asUploader(custom);
 *
 * // The returned IUploader supports both Kotlin suspend and Java CompletableFuture methods:
 * // - uploader.uploadAsyncJava(localSource)     -> CompletableFuture<BlobSource>
 * // - uploader.uploadManyAsyncJava(localSources) -> CompletableFuture<UploadResults>
 * ```
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
 *
 * The adapter bridges both directions:
 * - Kotlin callers use `uploadAsync` / `uploadManyAsync` (suspend functions)
 *   which internally await the [CompletableFuture] from the custom uploader.
 * - Java callers use `uploadAsyncJava` / `uploadManyAsyncJava` which return
 *   [CompletableFuture] directly.
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

    override fun uploadAsyncJava(
        local: LocalSource,
    ): CompletableFuture<BlobSource> =
        CoroutineScope(Dispatchers.IO).future { uploadAsync(local) }

    override fun uploadManyAsyncJava(
        localSources: List<LocalSource>,
    ): CompletableFuture<UploadResults> =
        CoroutineScope(Dispatchers.IO).future {
            uploadManyAsync(localSources)
        }

    override fun close() {
        customUploader.close()
    }
}
