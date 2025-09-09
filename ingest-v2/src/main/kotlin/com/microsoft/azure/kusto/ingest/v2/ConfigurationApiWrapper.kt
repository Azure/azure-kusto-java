/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.apis.DefaultApi
import com.microsoft.azure.kusto.ingest.v2.common.auth.TokenCredentialsProvider
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse

class ConfigurationApiWrapper(
    override val clusterUrl: String,
    override val tokenCredentialsProvider: TokenCredentialsProvider,
    override val skipSecurityChecks: Boolean = false,
) :
    KustoBaseApiClient(
        clusterUrl,
        tokenCredentialsProvider,
        skipSecurityChecks,
    ) {
    private val logger =
        org.slf4j.LoggerFactory.getLogger(
            ConfigurationApiWrapper::class.java,
        )
    private val api: DefaultApi =
        DefaultApi(
            baseUrl = "$clusterUrl/v1/rest/ingest",
            httpClientConfig = setupConfig,
        )

    suspend fun getConfigurationDetails(): ConfigurationResponse {
        val configurationHttpResponse: HttpResponse<ConfigurationResponse> =
            api.v1RestIngestionConfigurationGet()
        if (configurationHttpResponse.success) {
            logger.info(
                "Successfully retrieved configuration details from $clusterUrl with status: ${configurationHttpResponse.status}",
            )
            logger.debug(
                "Configuration details: {}",
                configurationHttpResponse.body(),
            )
            return configurationHttpResponse.body()
        } else {
            logger.error(
                "Failed to retrieve configuration details from $clusterUrl. Status: ${configurationHttpResponse.status}, " +
                    "Body: ${configurationHttpResponse.body()}",
            )
            throw IngestException(
                "Failed to retrieve configuration details from $clusterUrl. Status: ${configurationHttpResponse.status}, " +
                    "Body: ${configurationHttpResponse.body()}",
            )
        }
    }
}
