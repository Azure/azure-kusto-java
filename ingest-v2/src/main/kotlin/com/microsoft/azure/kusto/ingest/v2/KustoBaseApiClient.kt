// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.azure.core.credential.TokenRequestContext
import com.microsoft.azure.kusto.ingest.v2.apis.DefaultApi
import com.microsoft.azure.kusto.ingest.v2.auth.endpoints.KustoTrustedEndpoints
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.serialization.OffsetDateTimeSerializer
import io.ktor.client.HttpClientConfig
import io.ktor.client.plugins.DefaultRequest
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.client.plugins.auth.providers.bearer
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.header
import io.ktor.http.ContentType
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

open class KustoBaseApiClient(
    open val dmUrl: String,
    open val tokenCredential: TokenCredential,
    open val skipSecurityChecks: Boolean = false,
    open val clientDetails: ClientDetails,
    open val clientRequestIdPrefix: String = "KIC.execute",
    open val s2sTokenProvider: (suspend () -> Pair<String, String>)? = null,
    open val s2sFabricPrivateLinkAccessContext: String? = null,
) {
    private val logger = LoggerFactory.getLogger(KustoBaseApiClient::class.java)

    init {
        // Validate endpoint is trusted unless security checks are skipped
        // Note: dmUrl might be empty/null in some test scenarios (e.g., mocked clients)
        // The null check is required for Java interop - Java callers can pass null despite Kotlin's
        // non-null type
        if (!skipSecurityChecks && dmUrl != null && dmUrl.isNotBlank()) {
            KustoTrustedEndpoints.validateTrustedEndpoint(dmUrl)
        }
    }

    protected val setupConfig: (HttpClientConfig<*>) -> Unit = { config ->
        getClientConfig(config)
    }

    val engineUrl: String
        get() = dmUrl.replace(Regex("https://ingest-"), "https://")

    val api: DefaultApi by lazy {
        DefaultApi(baseUrl = dmUrl, httpClientConfig = setupConfig)
    }

    private fun getClientConfig(config: HttpClientConfig<*>) {
        config.install(DefaultRequest) {
            header("Content-Type", ContentType.Application.Json.toString())
            clientDetails.let { details ->
                header("x-ms-app", details.getApplicationForTracing())
                header("x-ms-user", details.getUserNameForTracing())
                header(
                    "x-ms-client-version",
                    details.getClientVersionForTracing(),
                )
            }

            // Generate unique client request ID for tracing (format: prefix;uuid)
            val clientRequestId = "$clientRequestIdPrefix;${UUID.randomUUID()}"
            header("x-ms-client-request-id", clientRequestId)
            header("x-ms-version", KUSTO_API_VERSION)
            header("Connection", "keep-alive")
            header("Accept", ContentType.Application.Json.toString())
        }
        val trc = TokenRequestContext().addScopes("$dmUrl/.default")
        config.install(Auth) {
            bearer {
                loadTokens {
                    // Always null so refreshTokens is always called
                    null
                }
                refreshTokens {
                    try {
                        // Use suspendCancellableCoroutine to convert Mono to suspend function
                        suspendCancellableCoroutine { continuation ->
                            tokenCredential
                                .getToken(trc)
                                .subscribe(
                                    { accessToken ->
                                        val bearerTokens =
                                            BearerTokens(
                                                accessToken =
                                                accessToken
                                                    .token,
                                                refreshToken =
                                                null,
                                            )
                                        continuation.resume(
                                            bearerTokens,
                                        )
                                    },
                                    { error ->
                                        continuation
                                            .resumeWithException(
                                                error,
                                            )
                                    },
                                )
                        }
                    } catch (e: Exception) {
                        // Handle token retrieval errors
                        logger.error(
                            "Error retrieving access token: ${e.message}",
                            e,
                        )
                        throw e
                    }
                }
            }
        }

        // Add S2S authorization and Fabric Private Link headers using request interceptor
        s2sTokenProvider?.let { provider ->
            config.install(
                io.ktor.client.plugins.api.createClientPlugin(
                    "S2SAuthPlugin",
                ) {
                    onRequest { request, _ ->
                        try {
                            // Get S2S token
                            val (token, scheme) = provider()
                            request.headers.append(
                                "x-ms-s2s-actor-authorization",
                                "$scheme $token",
                            )

                            // Add Fabric Private Link access context header
                            s2sFabricPrivateLinkAccessContext?.let { context,
                                ->
                                request.headers.append(
                                    "x-ms-fabric-s2s-access-context",
                                    context,
                                )
                            }
                        } catch (e: Exception) {
                            logger.error(
                                "Error retrieving S2S token: ${e.message}",
                                e,
                            )
                            throw e
                        }
                    }
                },
            )
        }
        config.install(ContentNegotiation) {
            json(
                Json {
                    ignoreUnknownKeys = true
                    serializersModule = SerializersModule {
                        contextual(
                            OffsetDateTime::class,
                            OffsetDateTimeSerializer,
                        )
                    }
                    // Optionally add other settings if needed:
                    isLenient = true
                    // allowSpecialFloatingPointValues = true
                    // useArrayPolymorphism = true
                },
            )
        }
        /* TODO Check what these settings should be */
        config.install(HttpTimeout) {
            requestTimeoutMillis = KUSTO_API_REQUEST_TIMEOUT_MS
            connectTimeoutMillis = KUSTO_API_CONNECT_TIMEOUT_MS
            socketTimeoutMillis = KUSTO_API_SOCKET_TIMEOUT_MS
        }
    }
}
