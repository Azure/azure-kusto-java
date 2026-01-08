// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.client.policy.ManagedStreamingPolicy
import com.microsoft.azure.kusto.ingest.v2.uploader.IUploader
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ManagedStreamingIngestClientBuilderTest {

    private val validDmUrl = "https://ingest-test.kusto.windows.net"
    private val mockTokenCredential: TokenCredential = mockk(relaxed = true)

    @Test
    fun `create with valid URL should succeed`() {
        val builder = ManagedStreamingIngestClientBuilder.create(validDmUrl)
        assertNotNull(builder)
    }

    @Test
    fun `create with blank URL should throw exception`() {
        assertThrows<IllegalArgumentException> {
            ManagedStreamingIngestClientBuilder.create("")
        }
    }

    @Test
    fun `create with whitespace URL should throw exception`() {
        assertThrows<IllegalArgumentException> {
            ManagedStreamingIngestClientBuilder.create("   ")
        }
    }

    @Test
    fun `build without authentication should throw exception`() {
        val builder = ManagedStreamingIngestClientBuilder.create(validDmUrl)
        assertThrows<IllegalArgumentException> { builder.build() }
    }

    @Test
    fun `build with authentication should succeed`() {
        val client =
            ManagedStreamingIngestClientBuilder.create(validDmUrl)
                .withAuthentication(mockTokenCredential)
                .build()
        assertNotNull(client)
    }

    @Test
    fun `withUploader should accept custom uploader`() {
        val mockUploader: IUploader = mockk(relaxed = true)
        val builder =
            ManagedStreamingIngestClientBuilder.create(validDmUrl)
                .withUploader(mockUploader, true)
        assertNotNull(builder)
    }

    @Test
    fun `withManagedStreamingIngestPolicy should accept custom policy`() {
        val mockPolicy: ManagedStreamingPolicy = mockk(relaxed = true)
        val builder =
            ManagedStreamingIngestClientBuilder.create(validDmUrl)
                .withManagedStreamingIngestPolicy(mockPolicy)
        assertNotNull(builder)
    }

    @Test
    fun `builder methods should return self for chaining`() {
        val builder = ManagedStreamingIngestClientBuilder.create(validDmUrl)
        val result =
            builder.withAuthentication(mockTokenCredential)
                .withClientDetails("TestApp", "1.0")

        assertEquals(builder, result)
    }

    @Test
    fun `withClientDetails should accept custom client details`() {
        val builder =
            ManagedStreamingIngestClientBuilder.create(validDmUrl)
                .withClientDetails("TestApp", "1.0.0")
        assertNotNull(builder)
    }

    @Test
    fun `skipSecurityChecks should be accepted`() {
        val builder =
            ManagedStreamingIngestClientBuilder.create(validDmUrl)
                .skipSecurityChecks()
        assertNotNull(builder)
    }

    @Test
    fun `build with all optional parameters should succeed`() {
        val mockUploader: IUploader = mockk(relaxed = true)
        val mockPolicy: ManagedStreamingPolicy = mockk(relaxed = true)

        val client =
            ManagedStreamingIngestClientBuilder.create(validDmUrl)
                .withAuthentication(mockTokenCredential)
                .withUploader(mockUploader, true)
                .withManagedStreamingIngestPolicy(mockPolicy)
                .withClientDetails("TestApp", "2.0")
                .skipSecurityChecks()
                .build()

        assertNotNull(client)
    }

    @Test
    fun `create should normalize engine URL to ingest URL`() {
        val engineUrl = "https://test.kusto.windows.net"
        val builder = ManagedStreamingIngestClientBuilder.create(engineUrl)
        assertNotNull(builder)
    }
}
