// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.ConfigurationClient
import com.microsoft.azure.kusto.ingest.v2.builders.ManagedStreamingIngestClientBuilder
import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder
import com.microsoft.azure.kusto.ingest.v2.builders.StreamingIngestClientBuilder
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.models.S2SToken
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Tests for Fabric Private Link (S2S authentication) support in the
 * configuration cache and client builders.
 */
class FabricPrivateLinkTest {

    private val validDmUrl = "https://ingest-test.kusto.windows.net"
    private val mockTokenCredential: TokenCredential = mockk(relaxed = true)
    private val testAccessContext = "test-fabric-access-context"
    private val testS2SScheme = "Bearer"
    private val testS2SToken = "s2s-test-token"

    private val mockS2sTokenProvider: suspend () -> S2SToken = {
        S2SToken(testS2SScheme, testS2SToken)
    }

    // ==================== DefaultConfigurationCache Tests ====================

    @Test
    fun `DefaultConfigurationCache should accept S2S parameters`() {
        val cache =
            DefaultConfigurationCache(
                dmUrl = validDmUrl,
                tokenCredential = mockTokenCredential,
                skipSecurityChecks = true,
                clientDetails = ClientDetails.createDefault(),
                s2sTokenProvider = mockS2sTokenProvider,
                s2sFabricPrivateLinkAccessContext = testAccessContext,
            )
        assertNotNull(cache)
    }

    @Test
    fun `DefaultConfigurationCache should work without S2S parameters`() {
        val cache =
            DefaultConfigurationCache(
                dmUrl = validDmUrl,
                tokenCredential = mockTokenCredential,
                skipSecurityChecks = true,
                clientDetails = ClientDetails.createDefault(),
            )
        assertNotNull(cache)
    }

    @Test
    fun `DefaultConfigurationCache should accept only S2S token provider`() {
        val cache =
            DefaultConfigurationCache(
                dmUrl = validDmUrl,
                tokenCredential = mockTokenCredential,
                skipSecurityChecks = true,
                clientDetails = ClientDetails.createDefault(),
                s2sTokenProvider = mockS2sTokenProvider,
            )
        assertNotNull(cache)
    }

    @Test
    fun `DefaultConfigurationCache should accept only access context`() {
        val cache =
            DefaultConfigurationCache(
                dmUrl = validDmUrl,
                tokenCredential = mockTokenCredential,
                skipSecurityChecks = true,
                clientDetails = ClientDetails.createDefault(),
                s2sFabricPrivateLinkAccessContext = testAccessContext,
            )
        assertNotNull(cache)
    }

    @Test
    fun `DefaultConfigurationCache with custom provider should work with S2S parameters`() {
        val mockConfigResponse: ConfigurationResponse = mockk(relaxed = true)
        val customProvider: suspend () -> ConfigurationResponse = {
            mockConfigResponse
        }

        val cache =
            DefaultConfigurationCache(
                clientDetails = ClientDetails.createDefault(),
                configurationProvider = customProvider,
                s2sTokenProvider = mockS2sTokenProvider,
                s2sFabricPrivateLinkAccessContext = testAccessContext,
            )

        runBlocking {
            val config = cache.getConfiguration()
            assertNotNull(config)
        }
    }

    // ==================== ConfigurationClient Tests ====================

    @Test
    fun `ConfigurationClient should accept S2S parameters`() {
        val client =
            ConfigurationClient(
                dmUrl = validDmUrl,
                tokenCredential = mockTokenCredential,
                skipSecurityChecks = true,
                clientDetails = ClientDetails.createDefault(),
                s2sTokenProvider = mockS2sTokenProvider,
                s2sFabricPrivateLinkAccessContext = testAccessContext,
            )
        assertNotNull(client)
        assertEquals(mockS2sTokenProvider, client.s2sTokenProvider)
        assertEquals(testAccessContext, client.s2sFabricPrivateLinkAccessContext)
    }

    @Test
    fun `ConfigurationClient should work without S2S parameters`() {
        val client =
            ConfigurationClient(
                dmUrl = validDmUrl,
                tokenCredential = mockTokenCredential,
                skipSecurityChecks = true,
                clientDetails = ClientDetails.createDefault(),
            )
        assertNotNull(client)
        assertNull(client.s2sTokenProvider)
        assertNull(client.s2sFabricPrivateLinkAccessContext)
    }

    // ==================== QueuedIngestClientBuilder Tests ====================

    @Test
    fun `QueuedIngestClientBuilder should accept withFabricPrivateLink`() {
        val builder =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withAuthentication(mockTokenCredential)
                .withFabricPrivateLink(mockS2sTokenProvider, testAccessContext)

        assertNotNull(builder)
    }

    @Test
    fun `QueuedIngestClientBuilder build with FabricPrivateLink should succeed`() {
        val client =
            QueuedIngestClientBuilder.create(validDmUrl)
                .withAuthentication(mockTokenCredential)
                .withFabricPrivateLink(mockS2sTokenProvider, testAccessContext)
                .build()

        assertNotNull(client)
    }

    @Test
    fun `QueuedIngestClientBuilder should chain withFabricPrivateLink correctly`() {
        val builder = QueuedIngestClientBuilder.create(validDmUrl)
        val result =
            builder
                .withAuthentication(mockTokenCredential)
                .withFabricPrivateLink(mockS2sTokenProvider, testAccessContext)
                .withMaxConcurrency(5)
                .skipSecurityChecks()

        assertEquals(builder, result)
    }

    // ==================== StreamingIngestClientBuilder Tests ====================

    @Test
    fun `StreamingIngestClientBuilder should accept withFabricPrivateLink`() {
        val builder =
            StreamingIngestClientBuilder.create(validDmUrl)
                .withAuthentication(mockTokenCredential)
                .withFabricPrivateLink(mockS2sTokenProvider, testAccessContext)

        assertNotNull(builder)
    }

    @Test
    fun `StreamingIngestClientBuilder build with FabricPrivateLink should succeed`() {
        val client =
            StreamingIngestClientBuilder.create(validDmUrl)
                .withAuthentication(mockTokenCredential)
                .withFabricPrivateLink(mockS2sTokenProvider, testAccessContext)
                .build()

        assertNotNull(client)
    }

    // ==================== ManagedStreamingIngestClientBuilder Tests ====================

    @Test
    fun `ManagedStreamingIngestClientBuilder should accept withFabricPrivateLink`() {
        val builder =
            ManagedStreamingIngestClientBuilder.create(validDmUrl)
                .withAuthentication(mockTokenCredential)
                .withFabricPrivateLink(mockS2sTokenProvider, testAccessContext)

        assertNotNull(builder)
    }

    @Test
    fun `ManagedStreamingIngestClientBuilder build with FabricPrivateLink should succeed`() {
        val client =
            ManagedStreamingIngestClientBuilder.create(validDmUrl)
                .withAuthentication(mockTokenCredential)
                .withFabricPrivateLink(mockS2sTokenProvider, testAccessContext)
                .build()

        assertNotNull(client)
    }

    @Test
    fun `ManagedStreamingIngestClientBuilder should chain withFabricPrivateLink correctly`() {
        val builder = ManagedStreamingIngestClientBuilder.create(validDmUrl)
        val result =
            builder
                .withAuthentication(mockTokenCredential)
                .withFabricPrivateLink(mockS2sTokenProvider, testAccessContext)
                .skipSecurityChecks()

        assertEquals(builder, result)
    }

    // ==================== Integration-like Tests ====================

    @Test
    fun `S2S token provider should be callable and return expected values`() {
        runBlocking {
            val s2sToken = mockS2sTokenProvider()
            assertEquals(testS2SScheme, s2sToken.scheme)
            assertEquals(testS2SToken, s2sToken.token)
            assertEquals("$testS2SScheme $testS2SToken", s2sToken.toHeaderValue())
        }
    }

    @Test
    fun `DefaultConfigurationCache with custom provider respects S2S context`() {
        var providerCalled = false
        val mockConfigResponse: ConfigurationResponse = mockk(relaxed = true)
        val customProvider: suspend () -> ConfigurationResponse = {
            providerCalled = true
            mockConfigResponse
        }

        val cache =
            DefaultConfigurationCache(
                clientDetails = ClientDetails.createDefault(),
                configurationProvider = customProvider,
                s2sTokenProvider = mockS2sTokenProvider,
                s2sFabricPrivateLinkAccessContext = testAccessContext,
            )

        runBlocking {
            cache.getConfiguration()
        }

        assertEquals(true, providerCalled)
    }
}
