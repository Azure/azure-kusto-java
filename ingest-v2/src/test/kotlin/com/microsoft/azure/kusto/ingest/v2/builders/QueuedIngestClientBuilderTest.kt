// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.uploader.IUploader
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class QueuedIngestClientBuilderTest {

    private val validDmUrl = "https://ingest-test.kusto.windows.net"
    private val mockTokenCredential: TokenCredential = mockk(relaxed = true)

    @Test
    fun `create with valid URL should succeed`() {
        val builder = QueuedIngestClientBuilder.create(validDmUrl)
        assertNotNull(builder)
    }

    @Test
    fun `create with blank URL should throw exception`() {
        assertThrows<IllegalArgumentException> {
            QueuedIngestClientBuilder.create("")
        }
    }

    @Test
    fun `create with whitespace URL should throw exception`() {
        assertThrows<IllegalArgumentException> {
            QueuedIngestClientBuilder.create("   ")
        }
    }

    @Test
    fun `withMaxConcurrency with positive value should succeed`() {
        val builder =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withMaxConcurrency(10)
        assertNotNull(builder)
    }

    @Test
    fun `withMaxConcurrency with zero should throw exception`() {
        val builder = QueuedIngestClientBuilder.create(validDmUrl)
        assertThrows<IllegalArgumentException> { builder.withMaxConcurrency(0) }
    }

    @Test
    fun `withMaxConcurrency with negative value should throw exception`() {
        val builder = QueuedIngestClientBuilder.create(validDmUrl)
        assertThrows<IllegalArgumentException> {
            builder.withMaxConcurrency(-1)
        }
    }

    @Test
    fun `withMaxDataSize with positive value should succeed`() {
        val builder =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withMaxDataSize(1024L)
        assertNotNull(builder)
    }

    @Test
    fun `withMaxDataSize with zero should throw exception`() {
        val builder = QueuedIngestClientBuilder.create(validDmUrl)
        assertThrows<IllegalArgumentException> { builder.withMaxDataSize(0L) }
    }

    @Test
    fun `withMaxDataSize with negative value should throw exception`() {
        val builder = QueuedIngestClientBuilder.create(validDmUrl)
        assertThrows<IllegalArgumentException> {
            builder.withMaxDataSize(-100L)
        }
    }

    @Test
    fun `withIgnoreFileSize should accept true`() {
        val builder =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withIgnoreFileSize(true)
        assertNotNull(builder)
    }

    @Test
    fun `withIgnoreFileSize should accept false`() {
        val builder =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withIgnoreFileSize(false)
        assertNotNull(builder)
    }

    @Test
    fun `withUploader should accept custom uploader`() {
        val mockUploader: IUploader = mockk(relaxed = true)
        val builder =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withUploader(mockUploader, true)
        assertNotNull(builder)
    }

    @Test
    fun `withConfiguration should accept custom configuration`() {
        val mockConfig =
            DefaultConfigurationCache(
                dmUrl = validDmUrl,
                tokenCredential = mockTokenCredential,
                skipSecurityChecks = false,
                clientDetails = ClientDetails.createDefault(),
            )
        val builder =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withConfiguration(mockConfig)
        assertNotNull(builder)
    }

    @Test
    fun `build without authentication should throw exception`() {
        val builder = QueuedIngestClientBuilder.create(validDmUrl)
        assertThrows<IllegalArgumentException> { builder.build() }
    }

    @Test
    fun `build with authentication should succeed`() {
        val client =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withAuthentication(mockTokenCredential)
                .build()
        assertNotNull(client)
    }

    @Test
    fun `builder methods should return self for chaining`() {
        val builder = QueuedIngestClientBuilder.create(validDmUrl)
        val result =
            builder.withMaxConcurrency(5)
                .withMaxDataSize(2048L)
                .withIgnoreFileSize(true)
                .withAuthentication(mockTokenCredential)

        assertEquals(builder, result)
    }

    @Test
    fun `create should normalize engine URL to ingest URL`() {
        val engineUrl = "https://test.kusto.windows.net"
        val builder = QueuedIngestClientBuilder.create(engineUrl)
        assertNotNull(builder)
    }

    @Test
    fun `withClientDetails should accept custom client details`() {
        val builder =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withClientDetails("TestApp", "1.0.0")
        assertNotNull(builder)
    }

    @Test
    fun `skipSecurityChecks should be accepted`() {
        val builder =
            QueuedIngestClientBuilder.create(validDmUrl)
                .skipSecurityChecks()
        assertNotNull(builder)
    }

    @Test
    fun `build with all optional parameters should succeed`() {
        val mockUploader: IUploader = mockk(relaxed = true)
        val mockConfig =
            DefaultConfigurationCache(
                dmUrl = validDmUrl,
                tokenCredential = mockTokenCredential,
                skipSecurityChecks = false,
                clientDetails = ClientDetails.createDefault(),
            )

        val client =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withAuthentication(mockTokenCredential)
                .withMaxConcurrency(10)
                .withMaxDataSize(4096L)
                .withIgnoreFileSize(true)
                .withUploader(mockUploader, true)
                .withConfiguration(mockConfig)
                .withClientDetails("TestApp", "2.0")
                .skipSecurityChecks()
                .build()

        assertNotNull(client)
    }
}
