// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.apis.DefaultApi
import com.microsoft.azure.kusto.ingest.v2.client.IngestionOperation
import com.microsoft.azure.kusto.ingest.v2.client.IngestProperties
import com.microsoft.azure.kusto.ingest.v2.client.toApiRequestProperties
import com.microsoft.azure.kusto.ingest.v2.common.IngestionMethod
import com.microsoft.azure.kusto.ingest.v2.common.auth.TokenCredentialsProvider
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.*
import com.microsoft.azure.kusto.ingest.v2.source.DataFormat
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime

class QueuedIngestionApiWrapper(
    override val dmUrl: String,
    override val tokenCredentialsProvider: TokenCredentialsProvider,
    override val skipSecurityChecks: Boolean = false,
) : KustoBaseApiClient(dmUrl, tokenCredentialsProvider, skipSecurityChecks) {
    
    private val logger = LoggerFactory.getLogger(QueuedIngestionApiWrapper::class.java)
    private val api: DefaultApi = DefaultApi(baseUrl = dmUrl, httpClientConfig = setupConfig)

    /**
     * Submits a queued ingestion request.
     * 
     * @param database The target database name
     * @param table The target table name
     * @param blobUrls List of blob URLs to ingest
     * @param format The data format
     * @param ingestProperties Optional ingestion properties
     * @return IngestionOperation for tracking the request
     */
    suspend fun submitQueuedIngestion(
        database: String,
        table: String,
        blobUrls: List<String>,
        format: DataFormat,
        ingestProperties: IngestProperties? = null
    ): IngestionOperation {
        logger.info("Submitting queued ingestion request for database: $database, table: $table, blobs: ${blobUrls.size}")
        
        // Convert blob URLs to Blob objects
        val blobs = blobUrls.mapIndexed { index, url ->
            Blob(
                url = url,
                sourceId = "source_${index}_${System.currentTimeMillis()}"
            )
        }
        
        // Use the domain model directly - no conversion needed!
        val apiFormat = Format.decode(format.kustoValue) 
            ?: throw IngestException("Unsupported format: ${format.kustoValue}")
        
        // Create IngestRequestProperties using simplified approach
        val requestProperties = ingestProperties?.toApiRequestProperties(apiFormat) 
            ?: IngestRequestProperties(format = apiFormat)
        
        // Create the ingestion request
        val ingestRequest = IngestRequest(
            timestamp = OffsetDateTime.now(),
            blobs = blobs,
            properties = requestProperties
        )
        
        try {
            val response: HttpResponse<IngestResponse> = api.v1RestIngestionQueuedDatabaseTablePost(
                database = database,
                table = table,
                ingestRequest = ingestRequest
            )
            
            if (response.success) {
                val operationId = response.body().ingestionOperationId
                    ?: throw IngestException("No operation ID returned from ingestion service")
                
                logger.info("Successfully submitted queued ingestion. Operation ID: $operationId")
                logger.debug("Ingestion response: {}", response.body())
                
                return IngestionOperation.create(
                    database = database,
                    table = table,
                    ingestionMethod = IngestionMethod.QUEUED,
                    operationId = operationId,
                    storedResults = emptyList()
                )
            } else {
                val errorMessage = "Failed to submit queued ingestion to $database.$table. " +
                    "Status: ${response.status}, Body: ${response.body()}"
                logger.error(errorMessage)
                throw IngestException(errorMessage)
            }
        } catch (e: Exception) {
            logger.error("Exception occurred during queued ingestion submission", e)
            if (e is IngestException) throw e
            throw IngestException("Failed to submit queued ingestion: ${e.message}", e)
        }
    }
    
    /**
     * Gets a summary of the ingestion operation status (lightweight, fast).
     * This method provides overall status counters without detailed blob information.
     * Use this for quick status checks and polling scenarios.
     * 
     * @param database The target database name
     * @param table The target table name
     * @param operationId The operation ID returned from the ingestion request
     * @return Updated IngestionOperation with status summary
     */
    suspend fun getIngestionSummary(
        database: String,
        table: String,
        operationId: String
    ): IngestionOperation {
        logger.debug("Checking ingestion summary for operation: $operationId")
        
        try {
            val response: HttpResponse<StatusResponse> = api.v1RestIngestionQueuedDatabaseTableOperationIdGet(
                database = database,
                table = table,
                operationId = operationId,
                details = false
            )
            
            if (response.success) {
                logger.debug("Successfully retrieved summary for operation: $operationId")
                logger.debug("Summary response: {}", response.body())
                
                return IngestionOperation.fromStatusResponse(
                    statusResponse = response.body(),
                    operationId = operationId,
                    database = database,
                    table = table,
                    ingestionMethod = IngestionMethod.QUEUED
                )
            } else {
                val errorMessage = "Failed to get ingestion summary for operation $operationId. " +
                    "Status: ${response.status}, Body: ${response.body()}"
                logger.error(errorMessage)
                throw IngestException(errorMessage)
            }
        } catch (e: Exception) {
            logger.error("Exception occurred while getting ingestion summary for operation: $operationId", e)
            if (e is IngestException) throw e
            throw IngestException("Failed to get ingestion summary: ${e.message}", e)
        }
    }

    /**
     * Gets detailed status of the ingestion operation including individual blob results.
     * This method provides comprehensive information including error details and per-blob status.
     * Use this when you need detailed diagnostic information or to handle specific failures.
     * 
     * @param database The target database name
     * @param table The target table name
     * @param operationId The operation ID returned from the ingestion request
     * @return Updated IngestionOperation with detailed status information
     */
    suspend fun getIngestionDetails(
        database: String,
        table: String,
        operationId: String
    ): IngestionOperation {
        logger.debug("Checking detailed ingestion status for operation: $operationId")
        
        try {
            val response: HttpResponse<StatusResponse> = api.v1RestIngestionQueuedDatabaseTableOperationIdGet(
                database = database,
                table = table,
                operationId = operationId,
                details = true
            )
            
            if (response.success) {
                logger.debug("Successfully retrieved details for operation: $operationId")
                logger.debug("Details response: {}", response.body())
                
                return IngestionOperation.fromStatusResponse(
                    statusResponse = response.body(),
                    operationId = operationId,
                    database = database,
                    table = table,
                    ingestionMethod = IngestionMethod.QUEUED
                )
            } else {
                val errorMessage = "Failed to get ingestion details for operation $operationId. " +
                    "Status: ${response.status}, Body: ${response.body()}"
                logger.error(errorMessage)
                throw IngestException(errorMessage)
            }
        } catch (e: Exception) {
            logger.error("Exception occurred while getting ingestion details for operation: $operationId", e)
            if (e is IngestException) throw e
            throw IngestException("Failed to get ingestion details: ${e.message}", e)
        }
    }

    /**
     * Gets the status of a queued ingestion operation with intelligent API selection.
     * For completed operations or when details are explicitly requested, uses the details API.
     * For in-progress operations, uses the summary API for efficiency.
     * 
     * @param database The target database name
     * @param table The target table name
     * @param operationId The operation ID returned from the ingestion request
     * @param forceDetails Force retrieval of detailed information regardless of operation status
     * @return Updated IngestionOperation with current status
     */
    suspend fun getIngestionStatus(
        database: String,
        table: String,
        operationId: String,
        forceDetails: Boolean = false
    ): IngestionOperation {
        // If details are explicitly requested, use the details API
        if (forceDetails) {
            return getIngestionDetails(database, table, operationId)
        }
        
        // Start with summary for efficiency
        val summary = getIngestionSummary(database, table, operationId)
        
        // If operation has failures or is completed, get detailed information
        return if (summary.statusCounts?.failed?.let { it > 0 } == true || summary.isCompleted) {
            logger.debug("Operation $operationId has failures or is completed, retrieving details")
            getIngestionDetails(database, table, operationId)
        } else {
            summary
        }
    }
}
