// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import org.slf4j.LoggerFactory

class ConfigurationClient(
    override val dmUrl: String,
    override val tokenCredential: TokenCredential,
    override val skipSecurityChecks: Boolean = false,
) : KustoBaseApiClient(dmUrl, tokenCredential, skipSecurityChecks) {
    private val logger =
        LoggerFactory.getLogger(ConfigurationClient::class.java)
    private val baseUrl = "$dmUrl/v1/rest/ingestion/configuration"

    suspend fun getConfigurationDetails(): ConfigurationResponse {
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
        } else {
            logger.error(
                "Failed to retrieve configuration details from $baseUrl. Status: ${configurationHttpResponse.status}, " +
                    "Body: ${configurationHttpResponse.body()}",
            )
            throw IngestException(
                "Failed to retrieve configuration details from $baseUrl. Status: ${configurationHttpResponse.status}, " +
                    "Body: ${configurationHttpResponse.body()}",
            )
        }
    }
}
