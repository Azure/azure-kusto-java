// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.azure.core.credential.TokenRequestContext
import io.ktor.client.HttpClientConfig
import io.ktor.client.plugins.DefaultRequest
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.client.plugins.auth.providers.bearer
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.header
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

open class KustoBaseApiClient(
    open val dmUrl: String,
    open val tokenCredential: TokenCredential,
    open val skipSecurityChecks: Boolean = false,
) {

    protected val setupConfig: (HttpClientConfig<*>) -> Unit = { config ->
        getClientConfig(config)
    }

    private fun getClientConfig(config: HttpClientConfig<*>) {
        config.install(DefaultRequest) {
            header("Content-Type", "application/json")
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
                                        val bearerTokens = BearerTokens(
                                            accessToken = accessToken.token,
                                            refreshToken = null,
                                        )
                                        continuation.resume(bearerTokens)
                                    },
                                    { error ->
                                        continuation.resumeWithException(error)
                                    }
                                )
                        }
                    } catch (e: Exception) {
                        // Handle token retrieval errors
                        null
                    }
                }
            }
        }
        config.install(ContentNegotiation) {
            json(
                Json {
                    ignoreUnknownKeys = true
                    // Optionally add other settings if needed:
                    // isLenient = true
                    // allowSpecialFloatingPointValues = true
                    // useArrayPolymorphism = true
                },
            )
        }
    }
}
