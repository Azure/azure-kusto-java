// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploaders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_CONCURRENCY
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.SimpleRetryPolicy

/** Builder for creating ManagedUploader instances with a fluent API. */
class ManagedUploaderBuilder private constructor() {
    private var ignoreSizeLimit: Boolean = false
    private var maxConcurrency: Int = UPLOAD_CONTAINER_MAX_CONCURRENCY
    private var maxDataSize: Long? = null
    private var configurationCache: ConfigurationCache? = null
    private var uploadMethod: UploadMethod = UploadMethod.DEFAULT
    private var ingestRetryPolicy: IngestRetryPolicy = SimpleRetryPolicy()
    private var tokenCredential: TokenCredential? = null

    companion object {
        /** Creates a new ManagedUploaderBuilder instance. */
        @JvmStatic
        fun create(): ManagedUploaderBuilder {
            return ManagedUploaderBuilder()
        }
    }

    /**
     * Sets whether to ignore the size limit for uploads.
     *
     * @param ignore true to ignore size limits, false to enforce them
     * @return this builder instance for method chaining
     */
    fun withIgnoreSizeLimit(ignore: Boolean): ManagedUploaderBuilder {
        this.ignoreSizeLimit = ignore
        return this
    }

    /**
     * Sets the maximum concurrency for parallel uploads.
     *
     * @param concurrency the maximum number of concurrent uploads
     * @return this builder instance for method chaining
     * @throws IllegalArgumentException if concurrency is not positive
     */
    fun withMaxConcurrency(concurrency: Int): ManagedUploaderBuilder {
        require(concurrency > 0) {
            "Max concurrency must be positive, got: $concurrency"
        }
        this.maxConcurrency = concurrency
        return this
    }

    /**
     * Sets the maximum data size for uploads in bytes.
     *
     * @param bytes the maximum data size in bytes
     * @return this builder instance for method chaining
     * @throws IllegalArgumentException if bytes is not positive
     */
    fun withMaxDataSize(bytes: Long): ManagedUploaderBuilder {
        require(bytes > 0) { "Max data size must be positive, got: $bytes" }
        this.maxDataSize = bytes
        return this
    }

    /**
     * Sets the configuration cache to use.
     *
     * @param cache the configuration cache instance
     * @return this builder instance for method chaining
     */
    fun withConfigurationCache(
        cache: ConfigurationCache,
    ): ManagedUploaderBuilder {
        this.configurationCache = cache
        return this
    }

    /**
     * Sets the upload method to use (Storage, Lake, or Default).
     *
     * @param method the upload method
     * @return this builder instance for method chaining
     */
    fun withUploadMethod(method: UploadMethod): ManagedUploaderBuilder {
        this.uploadMethod = method
        return this
    }

    /**
     * Sets the retry policy for ingestion operations.
     *
     * @param policy the retry policy to use
     * @return this builder instance for method chaining
     */
    fun withRetryPolicy(policy: IngestRetryPolicy): ManagedUploaderBuilder {
        this.ingestRetryPolicy = policy
        return this
    }

    /**
     * Sets the token credential for authentication.
     *
     * @param credential the token credential
     * @return this builder instance for method chaining
     */
    fun withTokenCredential(
        credential: TokenCredential,
    ): ManagedUploaderBuilder {
        this.tokenCredential = credential
        return this
    }

    /**
     * Builds and returns a ManagedUploader instance with the configured
     * settings.
     *
     * @return a new ManagedUploader instance
     * @throws IllegalStateException if required configuration is missing
     */
    fun build(): ManagedUploader {
        requireNotNull(configurationCache) {
            "Configuration cache is required. Call withConfigurationCache() before build()"
        }

        return ManagedUploader(
            ignoreSizeLimit = ignoreSizeLimit,
            maxConcurrency = maxConcurrency,
            maxDataSize =
            maxDataSize ?: UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES,
            configurationCache = configurationCache!!,
            uploadMethod = uploadMethod,
            ingestRetryPolicy = ingestRetryPolicy,
            tokenCredential = tokenCredential,
        )
    }
}
