// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import io.ktor.http.HttpStatusCode
import java.net.ConnectException

class StreamingIngestClient(
    override val dmUrl: String,
    override val tokenCredential: TokenCredential,
    override val skipSecurityChecks: Boolean = false,
) :
    KustoBaseApiClient(dmUrl, tokenCredential, skipSecurityChecks),
    IngestClient {

    /**
     * Submits a streaming ingestion request.
     *
     * @param database The target database name
     * @param table The target table name
     * @param data The data to ingest (as ByteArray)
     * @param format The data format
     * @param ingestProperties Optional ingestion properties
     * @return IngestResponse for tracking the request
     */
    suspend fun submitStreamingIngestion(
        database: String,
        table: String,
        data: ByteArray,
        format: Format = Format.csv,
        ingestProperties: IngestRequestProperties? = null,
    ) {
        logger.info(
            "Submitting streaming ingestion request for database: {}, table: {}, data size: {}",
            database,
            table,
            data.size,
        )
        try {
            val response: HttpResponse<Unit> =
                api.postStreamingIngest(
                    database = database,
                    table = table,
                    streamFormat = format,
                    body = data,
                    mappingName = ingestProperties?.mappingReference,
                    // TODO: What is sourceKind for streaming ingestion?
                    sourceKind = null,
                )
            return handleIngestResponse(
                response = response,
                database = database,
                table = table,
                dmUrl = dmUrl,
                endpointType = "streaming",
            )
        }
        catch (notAbleToReachHost: ConnectException) {
            val message =
                "Failed to reach $dmUrl for streaming ingestion. Please ensure the cluster address is correct and the cluster is reachable."
            throw IngestException(
                message = message,
                cause = notAbleToReachHost,
                failureCode = HttpStatusCode.NotFound.value,
                failureSubCode = "",
                isPermanent = false,
            )
        }
        catch (e: Exception) {
            logger.error(
                "Exception occurred during streaming ingestion submission",
                e,
            )
            if (e is IngestException) throw e
            throw IngestException(
                message =
                "Error submitting streaming ingest request to $dmUrl",
                cause = e,
                isPermanent = true,
            )
        }
    }
}
