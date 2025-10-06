// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import io.ktor.http.HttpStatusCode
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.ConnectException

interface IngestClient {
    val logger: Logger
        get() = LoggerFactory.getLogger(IngestClient::class.java)

    //Common way to parse ingestion response for both Streaming and Queued ingestion

    suspend fun <T : Any> handleIngestResponse(
        response: HttpResponse<T>,
        database: String,
        table: String,
        dmUrl: String,
        endpointType: String,
    ): T {
        if (response.success) {
            val ingestResponseBody = response.body()
            return ingestResponseBody
        } else {
            if (response.status == HttpStatusCode.NotFound.value) {
                val message =
                    "Endpoint $dmUrl not found. Please ensure the cluster supports $endpointType ingestion."
                logger.error(
                    "$endpointType ingestion endpoint not found. Please ensure that the target cluster supports $endpointType ingestion and that the endpoint URL is correct.",
                )
                throw IngestException(
                    message = message,
                    cause = ConnectException(message),
                    failureCode = response.status,
                    failureSubCode = "",
                    isPermanent = false,
                )
            }
            val nonSuccessResponseBody: T = response.body()
            val ingestResponseOperationId =
                if (nonSuccessResponseBody is IngestResponse) {
                    if (
                        (nonSuccessResponseBody as IngestResponse)
                            .ingestionOperationId != null
                    ) {
                        logger.info(
                            "Ingestion Operation ID: ${(nonSuccessResponseBody as IngestResponse).ingestionOperationId}",
                        )
                        nonSuccessResponseBody.ingestionOperationId
                    } else {
                        "N/A"
                    }
                } else {
                    "N/A"
                }
            val errorMessage =
                "Failed to submit $endpointType ingestion to $database.$table. " +
                    "Status: ${response.status}, Body: $nonSuccessResponseBody. " +
                    "OperationId $ingestResponseOperationId"
            logger.error(
                "$endpointType ingestion failed with response: {}",
                errorMessage,
            )
            throw IngestException(
                message = errorMessage,
                cause = RuntimeException(errorMessage),
                isPermanent = true,
            )
        }
    }
}
