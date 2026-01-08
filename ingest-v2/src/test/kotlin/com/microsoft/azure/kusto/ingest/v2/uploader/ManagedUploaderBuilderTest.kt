// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertNotNull

class ManagedUploaderBuilderTest {

    private val mockConfigurationCache: ConfigurationCache =
        mockk(relaxed = true)
    private val mockTokenCredential: TokenCredential = mockk(relaxed = true)
    private val mockRetryPolicy: IngestRetryPolicy = mockk(relaxed = true)

    @Test
    fun `create should return builder instance`() {
        val builder = ManagedUploaderBuilder.create()
        assertNotNull(builder)
    }

    @Test
    fun `withIgnoreSizeLimit true should succeed`() {
        val builder = ManagedUploaderBuilder.create().withIgnoreSizeLimit(true)
        assertNotNull(builder)
    }

    @Test
    fun `withIgnoreSizeLimit false should succeed`() {
        val builder = ManagedUploaderBuilder.create().withIgnoreSizeLimit(false)
        assertNotNull(builder)
    }

    @Test
    fun `withMaxConcurrency with positive value should succeed`() {
        val builder = ManagedUploaderBuilder.create().withMaxConcurrency(10)
        assertNotNull(builder)
    }

    @Test
    fun `withMaxConcurrency with zero should throw exception`() {
        val builder = ManagedUploaderBuilder.create()
        assertThrows<IllegalArgumentException> { builder.withMaxConcurrency(0) }
    }

    @Test
    fun `withMaxConcurrency with negative value should throw exception`() {
        val builder = ManagedUploaderBuilder.create()
        assertThrows<IllegalArgumentException> {
            builder.withMaxConcurrency(-1)
        }
    }

    @Test
    fun `withMaxDataSize with positive value should succeed`() {
        val builder = ManagedUploaderBuilder.create().withMaxDataSize(1024L)
        assertNotNull(builder)
    }

    @Test
    fun `withMaxDataSize with zero should throw exception`() {
        val builder = ManagedUploaderBuilder.create()
        assertThrows<IllegalArgumentException> { builder.withMaxDataSize(0L) }
    }

    @Test
    fun `withMaxDataSize with negative value should throw exception`() {
        val builder = ManagedUploaderBuilder.create()
        assertThrows<IllegalArgumentException> {
            builder.withMaxDataSize(-100L)
        }
    }

    @Test
    fun `withConfigurationCache should accept configuration`() {
        val builder =
            ManagedUploaderBuilder.create()
                .withConfigurationCache(mockConfigurationCache)
        assertNotNull(builder)
    }

    @Test
    fun `withUploadMethod STORAGE should succeed`() {
        val builder =
            ManagedUploaderBuilder.create()
                .withUploadMethod(UploadMethod.STORAGE)
        assertNotNull(builder)
    }

    @Test
    fun `withUploadMethod LAKE should succeed`() {
        val builder =
            ManagedUploaderBuilder.create()
                .withUploadMethod(UploadMethod.LAKE)
        assertNotNull(builder)
    }

    @Test
    fun `withUploadMethod DEFAULT should succeed`() {
        val builder =
            ManagedUploaderBuilder.create()
                .withUploadMethod(UploadMethod.DEFAULT)
        assertNotNull(builder)
    }

    @Test
    fun `withRetryPolicy should accept custom policy`() {
        val builder =
            ManagedUploaderBuilder.create().withRetryPolicy(mockRetryPolicy)
        assertNotNull(builder)
    }

    @Test
    fun `withTokenCredential should accept credential`() {
        val builder =
            ManagedUploaderBuilder.create()
                .withTokenCredential(mockTokenCredential)
        assertNotNull(builder)
    }

    @Test
    fun `build without configuration cache should throw exception`() {
        val builder = ManagedUploaderBuilder.create()
        assertThrows<IllegalStateException> { builder.build() }
    }

    @Test
    fun `build with configuration cache should succeed`() {
        val uploader =
            ManagedUploaderBuilder.create()
                .withConfigurationCache(mockConfigurationCache)
                .build()
        assertNotNull(uploader)
    }

    @Test
    fun `builder methods should return self for chaining`() {
        val builder = ManagedUploaderBuilder.create()
        val result =
            builder.withIgnoreSizeLimit(true)
                .withMaxConcurrency(5)
                .withMaxDataSize(2048L)
                .withConfigurationCache(mockConfigurationCache)
                .withUploadMethod(UploadMethod.STORAGE)
                .withRetryPolicy(mockRetryPolicy)
                .withTokenCredential(mockTokenCredential)

        assertNotNull(result)
    }

    @Test
    fun `build with all optional parameters should succeed`() {
        val uploader =
            ManagedUploaderBuilder.create()
                .withIgnoreSizeLimit(true)
                .withMaxConcurrency(10)
                .withMaxDataSize(4096L)
                .withConfigurationCache(mockConfigurationCache)
                .withUploadMethod(UploadMethod.LAKE)
                .withRetryPolicy(mockRetryPolicy)
                .withTokenCredential(mockTokenCredential)
                .build()

        assertNotNull(uploader)
    }

    @Test
    fun `build with minimal parameters should succeed`() {
        val uploader =
            ManagedUploaderBuilder.create()
                .withConfigurationCache(mockConfigurationCache)
                .build()

        assertNotNull(uploader)
    }
}
