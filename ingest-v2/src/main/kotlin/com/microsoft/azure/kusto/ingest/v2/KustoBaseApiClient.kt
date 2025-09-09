/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.auth.TokenCredentialsProvider
import io.ktor.client.HttpClientConfig
import io.ktor.client.plugins.DefaultRequest
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.client.plugins.auth.providers.bearer
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.header
import io.ktor.serialization.kotlinx.json.json

open class KustoBaseApiClient(
    open val clusterUrl: String,
    open val tokenCredentialsProvider: TokenCredentialsProvider,
    open val skipSecurityChecks: Boolean = false,
) {

    protected val setupConfig: (HttpClientConfig<*>) -> Unit = { config ->
        getClientConfig(config)
    }

    private fun getClientConfig(config: HttpClientConfig<*>) {
        config.install(DefaultRequest) {
            header("Content-Type", "application/json")
        }
        config.install(Auth) {
            bearer {
                loadTokens {
                    // Always null so refreshTokens is always called
                    tokenCredentialsProvider
                        .getCredentialsAsync(clusterUrl)
                        .tokenValue
                        ?.let {
                            BearerTokens(
                                accessToken = it,
                                refreshToken = null,
                            )
                        }
                }
                refreshTokens {
                    // Always null so refreshTokens is always called
                    tokenCredentialsProvider
                        .getCredentialsAsync(clusterUrl)
                        .tokenValue
                        ?.let {
                            BearerTokens(
                                accessToken = it,
                                refreshToken = null,
                            )
                        }
                }
            }
        }
        config.install(ContentNegotiation) { json() }
    }
}
