// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.AccessToken
import com.azure.core.credential.TokenCredential
import com.azure.core.credential.TokenRequestContext
import com.microsoft.azure.kusto.ingest.v2.auth.endpoints.KustoTrustedEndpoints
import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder
import com.microsoft.azure.kusto.ingest.v2.builders.StreamingIngestClientBuilder
import com.microsoft.azure.kusto.ingest.v2.exceptions.KustoClientInvalidConnectionStringException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import reactor.core.publisher.Mono
import java.time.OffsetDateTime
import kotlin.test.assertTrue

/**
 * Tests for endpoint validation functionality in ingest-v2.
 *
 * These tests verify that:
 * 1. Adhoc/untrusted Kusto endpoints are blocked by default
 * 2. The skipSecurityChecks() method allows untrusted endpoints
 * 3. Trusted Kusto endpoints are allowed by default
 *
 * The validation is now implemented natively in ingest-v2 using:
 * - WellKnownKustoEndpoints.json (copied from data module during build)
 * - KustoTrustedEndpoints object for validation logic
 * - KustoClientInvalidConnectionStringException for untrusted endpoints
 *
 * Note: This test class uses SAME_THREAD execution mode to prevent race
 * conditions when modifying the global enableWellKnownKustoEndpointsValidation
 * flag.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TrustedEndpointValidationTest {

    // Mock token credential for testing
    private val mockTokenCredential =
        TokenCredential { _: TokenRequestContext ->
            Mono.just(
                AccessToken(
                    "mock-token",
                    OffsetDateTime.now().plusHours(1),
                ),
            )
        }

    // Example of an adhoc/untrusted endpoint
    private val untrustedEndpoint =
        "https://my-random-adhoc-cluster.example.com"

    // Example of a trusted Kusto endpoint (public cloud)
    private val trustedEndpoint = "https://mycluster.kusto.windows.net"

    // Store original validation state to restore after tests
    private var originalValidationState: Boolean = true

    @BeforeEach
    fun setUp() {
        // Save original state and ensure validation is enabled
        originalValidationState =
            KustoTrustedEndpoints.enableWellKnownKustoEndpointsValidation
        KustoTrustedEndpoints.enableWellKnownKustoEndpointsValidation = true
    }

    @AfterEach
    fun tearDown() {
        // Restore original validation state
        KustoTrustedEndpoints.enableWellKnownKustoEndpointsValidation =
            originalValidationState
    }

    // ============================================================================
    // UNTRUSTED ENDPOINT TESTS - Should throw exception
    // ============================================================================

    @Test
    @DisplayName(
        "StreamingIngestClient: Untrusted endpoint throws exception without skipSecurityChecks",
    )
    fun `streaming client - untrusted endpoint throws without skip security checks`() {
        val exception =
            assertThrows<KustoClientInvalidConnectionStringException> {
                StreamingIngestClientBuilder.create(untrustedEndpoint)
                    .withAuthentication(mockTokenCredential)
                    // Note: NOT calling skipSecurityChecks()
                    .build()
            }

        assertTrue(
            exception.message?.contains("not trusted") == true ||
                exception.message?.contains("kustotrustedendpoints") ==
                true,
            "Exception should indicate endpoint is not trusted. Actual: ${exception.message}",
        )
    }

    @Test
    @DisplayName(
        "QueuedIngestClient: Untrusted endpoint throws exception without skipSecurityChecks",
    )
    fun `queued client - untrusted endpoint throws without skip security checks`() {
        val exception =
            assertThrows<KustoClientInvalidConnectionStringException> {
                QueuedIngestClientBuilder.create(untrustedEndpoint)
                    .withAuthentication(mockTokenCredential)
                    // Note: NOT calling skipSecurityChecks()
                    .build()
            }

        assertTrue(
            exception.message?.contains("not trusted") == true ||
                exception.message?.contains("kustotrustedendpoints") ==
                true,
            "Exception should indicate endpoint is not trusted. Actual: ${exception.message}",
        )
    }

    // ============================================================================
    // SKIP SECURITY CHECKS TESTS - Should work with the flag
    // ============================================================================

    @Test
    @DisplayName(
        "StreamingIngestClient: Untrusted endpoint works with skipSecurityChecks",
    )
    fun `streaming client - untrusted endpoint works with skip security checks`() {
        assertDoesNotThrow {
            StreamingIngestClientBuilder.create(untrustedEndpoint)
                .withAuthentication(mockTokenCredential)
                .skipSecurityChecks()
                .build()
        }
    }

    @Test
    @DisplayName(
        "QueuedIngestClient: Untrusted endpoint works with skipSecurityChecks",
    )
    fun `queued client - untrusted endpoint works with skip security checks`() {
        assertDoesNotThrow {
            QueuedIngestClientBuilder.create(untrustedEndpoint)
                .withAuthentication(mockTokenCredential)
                .skipSecurityChecks()
                .build()
        }
    }

    // ============================================================================
    // TRUSTED ENDPOINT TESTS - Should work without skipSecurityChecks
    // ============================================================================

    @Test
    @DisplayName(
        "StreamingIngestClient: Trusted Kusto endpoint works without skipSecurityChecks",
    )
    fun `streaming client - trusted endpoint works without skip security checks`() {
        assertDoesNotThrow {
            StreamingIngestClientBuilder.create(trustedEndpoint)
                .withAuthentication(mockTokenCredential)
                .build()
        }
    }

    @Test
    @DisplayName(
        "QueuedIngestClient: Trusted Kusto endpoint works without skipSecurityChecks",
    )
    fun `queued client - trusted endpoint works without skip security checks`() {
        assertDoesNotThrow {
            QueuedIngestClientBuilder.create(trustedEndpoint)
                .withAuthentication(mockTokenCredential)
                .build()
        }
    }

    // ============================================================================
    // GLOBAL FLAG TESTS
    // ============================================================================

    @Test
    @DisplayName("Global validation flag can disable endpoint checks")
    fun `global validation flag disables endpoint checks`() {
        // Disable validation globally
        KustoTrustedEndpoints.enableWellKnownKustoEndpointsValidation = false

        try {
            // Now untrusted endpoints should work even without skipSecurityChecks
            assertDoesNotThrow {
                StreamingIngestClientBuilder.create(untrustedEndpoint)
                    .withAuthentication(mockTokenCredential)
                    .build()
            }
        } finally {
            // Re-enable for other tests
            KustoTrustedEndpoints.enableWellKnownKustoEndpointsValidation = true
        }
    }

    // ============================================================================
    // CLOUD-SPECIFIC ENDPOINT TESTS
    // ============================================================================

    @Test
    @DisplayName("Various cloud-specific endpoints are trusted")
    fun `cloud specific endpoints are trusted`() {
        val trustedEndpoints =
            listOf(
                // Public cloud
                "https://mycluster.kusto.windows.net",
                "https://mycluster.kustodev.windows.net",
                "https://mycluster.kustomfa.windows.net",
                // Fabric
                "https://mycluster.kusto.fabric.microsoft.com",
                // Synapse
                "https://mycluster.kusto.azuresynapse.net",
            )

        trustedEndpoints.forEach { endpoint ->
            assertDoesNotThrow("Endpoint $endpoint should be trusted") {
                StreamingIngestClientBuilder.create(endpoint)
                    .withAuthentication(mockTokenCredential)
                    .build()
            }
        }
    }

    @Test
    @DisplayName("Localhost endpoints are always trusted")
    fun `localhost endpoints are trusted`() {
        val localhostEndpoints =
            listOf(
                "https://localhost:8080",
                "https://127.0.0.1:8080",
                "https://localhost",
            )

        localhostEndpoints.forEach { endpoint ->
            assertDoesNotThrow(
                "Localhost endpoint $endpoint should be trusted",
            ) {
                StreamingIngestClientBuilder.create(endpoint)
                    .withAuthentication(mockTokenCredential)
                    .build()
            }
        }
    }

    // ============================================================================
    // DIRECT API TESTS - Test KustoTrustedEndpoints directly
    // ============================================================================

    @Test
    @DisplayName("KustoTrustedEndpoints.isTrusted returns correct values")
    fun `isTrusted returns correct values`() {
        assertTrue(
            KustoTrustedEndpoints.isTrusted("mycluster.kusto.windows.net"),
            "Public cloud endpoint should be trusted",
        )
        assertTrue(
            KustoTrustedEndpoints.isTrusted(
                "mycluster.kusto.fabric.microsoft.com",
            ),
            "Fabric endpoint should be trusted",
        )
        assertTrue(
            KustoTrustedEndpoints.isTrusted("localhost"),
            "Localhost should be trusted",
        )
        assertTrue(
            !KustoTrustedEndpoints.isTrusted("random.example.com"),
            "Random endpoint should not be trusted",
        )
    }

    @Test
    @DisplayName(
        "KustoTrustedEndpoints.validateTrustedEndpoint throws for untrusted",
    )
    fun `validateTrustedEndpoint throws for untrusted endpoints`() {
        assertThrows<KustoClientInvalidConnectionStringException> {
            KustoTrustedEndpoints.validateTrustedEndpoint(
                "https://evil.example.com",
            )
        }
    }

    @Test
    @DisplayName(
        "KustoTrustedEndpoints.validateTrustedEndpoint passes for trusted",
    )
    fun `validateTrustedEndpoint passes for trusted endpoints`() {
        assertDoesNotThrow {
            KustoTrustedEndpoints.validateTrustedEndpoint(
                "https://mycluster.kusto.windows.net",
            )
        }
    }
}
