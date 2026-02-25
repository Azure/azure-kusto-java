// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.AccessToken
import com.azure.core.credential.TokenCredential
import com.azure.core.credential.TokenRequestContext
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import io.ktor.client.plugins.auth.providers.BearerTokens
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for KustoBaseApiClient, specifically the token refresh logic.
 */
class KustoBaseApiClientTest {

    @Test
    @DisplayName("getBearerToken should retrieve token from TokenCredential")
    fun `getBearerToken retrieves token from TokenCredential`(): Unit = runBlocking {
        // Arrange
        val expectedToken = "test-access-token"
        val tokenCallCount = AtomicInteger(0)

        val mockTokenCredential = TokenCredential { _: TokenRequestContext ->
            tokenCallCount.incrementAndGet()
            Mono.just(AccessToken(expectedToken, OffsetDateTime.now().plusHours(1)))
        }

        val clientDetails = ClientDetails(
            applicationForTracing = "TestApp",
            userNameForTracing = "TestUser",
            clientVersionForTracing = "1.0.0"
        )

        val client = TestableKustoBaseApiClient(
            dmUrl = "https://ingest-test.westus.kusto.windows.net",
            tokenCredential = mockTokenCredential,
            skipSecurityChecks = true,
            clientDetails = clientDetails
        )

        val trc = TokenRequestContext().addScopes("https://ingest-test.westus.kusto.windows.net/.default")

        // Act
        val bearerTokens = client.testGetBearerToken(trc)

        // Assert
        assertNotNull(bearerTokens)
        assertEquals(expectedToken, bearerTokens.accessToken)
        assertEquals(1, tokenCallCount.get(), "getToken should be called exactly once")
    }

    @Test
    @DisplayName("getBearerToken should be called multiple times when token needs refresh")
    fun `getBearerToken is called multiple times for token refresh`(): Unit = runBlocking {
        // Arrange
        val firstToken = "first-token"
        val secondToken = "second-token"
        val tokenCallCount = AtomicInteger(0)

        val mockTokenCredential = TokenCredential { _: TokenRequestContext ->
            val callNumber = tokenCallCount.incrementAndGet()
            if (callNumber == 1) {
                // First token expires in 1 second (simulating an expired token)
                Mono.just(AccessToken(firstToken, OffsetDateTime.now().plusSeconds(1)))
            } else {
                // Second token expires in 1 hour
                Mono.just(AccessToken(secondToken, OffsetDateTime.now().plusHours(1)))
            }
        }

        val clientDetails = ClientDetails(
            applicationForTracing = "TestApp",
            userNameForTracing = "TestUser",
            clientVersionForTracing = "1.0.0"
        )

        val client = TestableKustoBaseApiClient(
            dmUrl = "https://ingest-test.westus.kusto.windows.net",
            tokenCredential = mockTokenCredential,
            skipSecurityChecks = true,
            clientDetails = clientDetails
        )

        val trc = TokenRequestContext().addScopes("https://ingest-test.westus.kusto.windows.net/.default")

        // Act - First call
        val firstBearerTokens = client.testGetBearerToken(trc)

        // Act - Second call (simulating refresh after expiry)
        val secondBearerTokens = client.testGetBearerToken(trc)

        // Assert
        assertNotNull(firstBearerTokens)
        assertEquals(firstToken, firstBearerTokens.accessToken)

        assertNotNull(secondBearerTokens)
        assertEquals(secondToken, secondBearerTokens.accessToken)

        // Verify getToken was called twice
        assertEquals(2, tokenCallCount.get(), "getToken should be called twice for token refresh")
    }

    @Test
    @DisplayName("getBearerToken should handle token credential errors")
    fun `getBearerToken handles token credential errors`(): Unit = runBlocking {
        // Arrange
        val expectedMessage = "Token retrieval failed"
        val tokenCallCount = AtomicInteger(0)

        val mockTokenCredential = TokenCredential { _: TokenRequestContext ->
            tokenCallCount.incrementAndGet()
            Mono.error(RuntimeException(expectedMessage))
        }

        val clientDetails = ClientDetails(
            applicationForTracing = "TestApp",
            userNameForTracing = "TestUser",
            clientVersionForTracing = "1.0.0"
        )

        val client = TestableKustoBaseApiClient(
            dmUrl = "https://ingest-test.westus.kusto.windows.net",
            tokenCredential = mockTokenCredential,
            skipSecurityChecks = true,
            clientDetails = clientDetails
        )

        val trc = TokenRequestContext().addScopes("https://ingest-test.westus.kusto.windows.net/.default")

        // Act & Assert
        var exceptionCaught = false
        var caughtMessage: String? = null
        try {
            client.testGetBearerToken(trc)
        } catch (e: Exception) {
            exceptionCaught = true
            caughtMessage = e.message
        }

        assertTrue(exceptionCaught, "Expected exception should be thrown")
        assertEquals(expectedMessage, caughtMessage, "Exception message should match")
        assertEquals(1, tokenCallCount.get(), "getToken should be called exactly once before failing")
    }

    /**
     * Testable subclass that exposes the protected getBearerToken method for testing.
     */
    private class TestableKustoBaseApiClient(
        dmUrl: String,
        tokenCredential: TokenCredential,
        skipSecurityChecks: Boolean,
        clientDetails: ClientDetails
    ) : KustoBaseApiClient(
        dmUrl = dmUrl,
        tokenCredential = tokenCredential,
        skipSecurityChecks = skipSecurityChecks,
        clientDetails = clientDetails
    ) {
        // Expose the protected getBearerToken method for testing
        suspend fun testGetBearerToken(tokenRequestContext: TokenRequestContext): BearerTokens {
            return super.getBearerToken(tokenRequestContext)
        }
    }
}



