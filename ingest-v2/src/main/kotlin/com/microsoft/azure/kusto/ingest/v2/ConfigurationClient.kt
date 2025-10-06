// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import io.ktor.http.HttpStatusCode
import org.slf4j.LoggerFactory
import java.net.ConnectException

class ConfigurationClient(
    override val dmUrl: String,
    override val tokenCredential: TokenCredential,
    override val skipSecurityChecks: Boolean = false,
) : KustoBaseApiClient(dmUrl, tokenCredential, skipSecurityChecks) {
    private val logger =
        LoggerFactory.getLogger(ConfigurationClient::class.java)
    private val baseUrl = "$dmUrl/v1/rest/ingestion/configuration"

    suspend fun getConfigurationDetails(): ConfigurationResponse {
        try {
            val configurationHttpResponse: HttpResponse<ConfigurationResponse> =
                api.getIngestConfiguration()
            if (configurationHttpResponse.success) {
                logger.info(
                    "Successfully retrieved configuration details from $dmUrl with status: ${configurationHttpResponse.status}",
                )
                logger.debug(
                    "Configuration details: {}",
                    configurationHttpResponse.body(),
                )
                return configurationHttpResponse.body()
            } else if (
                configurationHttpResponse.status ==
                HttpStatusCode.NotFound.value
            ) {
                /*
                404 is a special case - it indicates that the endpoint is not found. This may be a transient
                network issue
                 */
                val message =
                    "Endpoint $dmUrl not found. Please ensure the cluster supports queued ingestion."
                logger.error(
                    "{}. Status: {}",
                    message,
                    configurationHttpResponse.status,
                )
                throw IngestException(
                    message = message,
                    cause = ConnectException(message),
                    failureCode = configurationHttpResponse.status,
                    failureSubCode = "",
                    isPermanent = false,
                )
            } else {
                val configurationResponseBody = configurationHttpResponse.body()
                val message =
                    "Failed to retrieve configuration details from $baseUrl.Status: ${configurationHttpResponse.status}, " +
                        "Body: $configurationResponseBody"
                logger.error("{}", message)
                throw IngestException(
                    message = message,
                    failureCode = configurationHttpResponse.status,
                )
            }
        } catch (notAbleToReachHost: ConnectException) {
            val message =
                "Failed to reach $baseUrl. Please ensure the cluster address is correct and the cluster is reachable."
            throw IngestException(
                message = message,
                cause = notAbleToReachHost,
                failureCode = HttpStatusCode.NotFound.value,
                failureSubCode = "",
                isPermanent = false,
            )
        } catch (ex: IngestException) {
            // if thrown from the body, rethrow this
            throw ex
        } catch (ex: Exception) {
            val message =
                "An unexpected error occurred while trying to reach $baseUrl"
            throw IngestException(
                message = message,
                cause = ex,
                // Mark this as a 5xx series error
                failureCode = HttpStatusCode.InternalServerError.value,
                failureSubCode = "",
                isPermanent = true,
            )
        }
    }
}
