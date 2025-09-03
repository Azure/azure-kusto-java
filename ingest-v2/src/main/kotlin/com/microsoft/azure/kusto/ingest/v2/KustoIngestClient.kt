package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.SimpleRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.auth.KustoTokenCredentialsProvider
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.models.KustoTokenCredentials
import io.ktor.client.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.auth.*
import io.ktor.client.plugins.auth.providers.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.client.statement.bodyAsText
import io.ktor.client.statement.HttpResponse
import io.ktor.http.HttpMethod
import kotlinx.serialization.json.Json
import kotlinx.serialization.decodeFromString
import kotlin.reflect.KClass
import kotlinx.coroutines.CancellationException

import java.net.URI

open class KustoIngestClient(
    val clusterUrl: String,
    val clientDetails: ClientDetails,
    val kustoTokenCredentials: KustoTokenCredentials?,
    retryPolicy: IngestRetryPolicy? = null,
    val skipSecurityChecks: Boolean = false
) {
    val retryPolicy: IngestRetryPolicy = retryPolicy ?: SimpleRetryPolicy()
    private var authInitialized = false
    private var audience: String = "https://kusto.kusto.windows.net"


    init {
        if (!skipSecurityChecks) {
            val uri = URI(clusterUrl)
            val scheme = uri.scheme?.lowercase()
            if (!(scheme == "https" || (scheme == "http" && kustoTokenCredentials != null))) {
                throw IllegalArgumentException("The provided endpoint is not a valid endpoint")
            }
        }
    }


    protected val setupConfig: suspend (HttpClientConfig<*>) -> Unit = { config -> getClientConfig(config) }

    private suspend fun getClientConfig(config: HttpClientConfig<*>) {
        config.install(DefaultRequest) {
            header("Content-Type", "application/json")
        }

        kustoTokenCredentials!!.tokenValue?.let { bearerToken ->
            config.install(Auth) {
                bearer {
                    loadTokens { BearerTokens(bearerToken, refreshToken = "") }
                }
            }
        }
        config.install(ContentNegotiation) {
            json()
        }
    }

    // Authenticates the request by setting the Authorization header using the token provider.
    suspend fun authenticate(request: HttpRequestBuilder) {
        if (kustoTokenCredentials == null) return
        if (!authInitialized) {
            authInitialized = true
            // For now, use a constant for the audience as a placeholder
        }
        request.headers.append("Authorization", "Bearer ${kustoTokenCredentials.tokenValue}")
    }
}