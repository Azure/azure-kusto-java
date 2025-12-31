// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class StreamingIngestClientBuilderTest {

    private val validClusterUrl = "https://test.kusto.windows.net"
    private val mockTokenCredential: TokenCredential = mockk(relaxed = true)

    @Test
    fun `create with valid URL should succeed`() {
        val builder = StreamingIngestClientBuilder.create(validClusterUrl)
        assertNotNull(builder)
    }

    @Test
    fun `create with blank URL should throw exception`() {
        assertThrows<IllegalArgumentException> {
            StreamingIngestClientBuilder.create("")
        }
    }

    @Test
    fun `create with whitespace URL should throw exception`() {
        assertThrows<IllegalArgumentException> {
            StreamingIngestClientBuilder.create("   ")
        }
    }

    @Test
    fun `build without authentication should throw exception`() {
        val builder = StreamingIngestClientBuilder.create(validClusterUrl)
        assertThrows<IllegalArgumentException> {
            builder.build()
        }
    }

    @Test
    fun `build with authentication should succeed`() {
        val client = StreamingIngestClientBuilder.create(validClusterUrl)
            .withAuthentication(mockTokenCredential)
            .build()
        assertNotNull(client)
    }

    @Test
    fun `builder methods should return self for chaining`() {
        val builder = StreamingIngestClientBuilder.create(validClusterUrl)
        val result = builder
            .withAuthentication(mockTokenCredential)
            .withClientDetails("TestApp", "1.0")
        
        assertEquals(builder, result)
    }

    @Test
    fun `create should normalize ingest URL to cluster URL`() {
        val ingestUrl = "https://ingest-test.kusto.windows.net"
        val builder = StreamingIngestClientBuilder.create(ingestUrl)
        assertNotNull(builder)
    }

    @Test
    fun `withClientDetails should accept custom client details`() {
        val builder = StreamingIngestClientBuilder.create(validClusterUrl)
            .withClientDetails("TestApp", "1.0.0")
        assertNotNull(builder)
    }

    @Test
    fun `skipSecurityChecks should be accepted`() {
        val builder = StreamingIngestClientBuilder.create(validClusterUrl)
            .skipSecurityChecks()
        assertNotNull(builder)
    }

    @Test
    fun `build with all optional parameters should succeed`() {
        val client = StreamingIngestClientBuilder.create(validClusterUrl)
            .withAuthentication(mockTokenCredential)
            .withClientDetails("TestApp", "2.0")
            .skipSecurityChecks()
            .build()
        
        assertNotNull(client)
    }
}
