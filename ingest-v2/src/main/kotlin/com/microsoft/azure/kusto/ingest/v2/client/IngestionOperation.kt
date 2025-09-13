// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.common.IngestionMethod
import com.microsoft.azure.kusto.ingest.v2.common.serialization.OffsetDateTimeSerializer
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.models.Status
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import java.time.OffsetDateTime
import java.util.*

/**
 * The result of an ingestion operation that was submitted to Kusto.
 * This object is used to track the status of the ingestion operation.
 */
@Serializable
data class IngestionOperation(
    /**
     * A unique identifier of the ingestion operation.
     */
    val id: String,

    /**
     * The ingestion method that was performed - either Queued or Streaming.
     */
    val ingestionMethod: IngestionMethod,

    /**
     * The database where the data is being ingested.
     */
    val database: String,

    /**
     * The table where the data is being ingested.
     */
    val table: String,

    /**
     * The start time of the ingestion operation.
     */
    @Contextual
    val startTime: OffsetDateTime,

    /**
     * The list of results of the ingestion operation.
     * In case of Queued ingestion when tracking enabled, the list will contain only failed results.
     * In case of Queued ingestion when tracking disabled, the list will contain failed results and succeeded results which indicates the upload succeeded (no information on ingestion success).
     * In case of Streaming ingestion, the list will contain all results.
     */
    @SerialName("storedResults")
    val storedResults: List<BlobStatus> = emptyList(),

    /**
     * Status counters tracking the count of each ingestion status.
     */
    @SerialName("statusCounts")
    val statusCounts: Status? = null,

    /**
     * Indicates whether this operation ID came from the data management service.
     */
    @SerialName("isDataManagementOperationId")
    val isDataManagementOperationId: Boolean
) {
    
    /**
     * Constructor for creating an IngestionOperation with basic information.
     */
    constructor(
        database: String,
        table: String,
        ingestionMethod: IngestionMethod,
        operationId: String? = null,
        storedResults: List<BlobStatus> = emptyList()
    ) : this(
        id = operationId ?: UUID.randomUUID().toString(),
        ingestionMethod = ingestionMethod,
        database = database,
        table = table,
        startTime = calculateStartTime(storedResults),
        storedResults = storedResults,
        statusCounts = calculateStatusCounts(storedResults),
        isDataManagementOperationId = !operationId.isNullOrBlank()
    )

    /**
     * Serializes the object to a JSON string.
     * Used to store the object in a persistent storage, to be able to be queried later.
     * See fromJsonString() to deserialize the object.
     */
    fun toJsonString(): String {
        return jsonInstance.encodeToString(serializer(), this)
    }

    companion object {
        private val jsonInstance = Json {
            serializersModule = SerializersModule {
                contextual(OffsetDateTime::class, OffsetDateTimeSerializer)
            }
        }

        /**
         * Deserializes the object from a JSON string.
         * Used to restore the object from a persistent storage.
         * See toJsonString() to serialize the object.
         */
        fun fromJsonString(jsonString: String): IngestionOperation {
            return jsonInstance.decodeFromString(serializer(), jsonString)
        }

        /**
         * Creates an IngestionOperation instance.
         */
        fun create(
            database: String,
            table: String,
            ingestionMethod: IngestionMethod,
            operationId: String? = null,
            storedResults: List<BlobStatus> = emptyList()
        ): IngestionOperation {
            return IngestionOperation(database, table, ingestionMethod, operationId, storedResults)
        }

        /**
         * Converts a StatusResponse from the API to an IngestionOperation for tracking.
         */
        fun fromStatusResponse(
            statusResponse: com.microsoft.azure.kusto.ingest.v2.models.StatusResponse,
            operationId: String,
            database: String,
            table: String,
            ingestionMethod: IngestionMethod
        ): IngestionOperation {
            return IngestionOperation(
                id = operationId,
                ingestionMethod = ingestionMethod,
                database = database,
                table = table,
                startTime = statusResponse.startTime ?: OffsetDateTime.now(),
                storedResults = statusResponse.details ?: emptyList(),
                statusCounts = statusResponse.status,
                isDataManagementOperationId = true
            )
        }

        private fun calculateStartTime(storedResults: List<BlobStatus>): OffsetDateTime {
            val minStartTime = storedResults
                .mapNotNull { it.startedAt }
                .minOrNull()
            return minStartTime ?: OffsetDateTime.now()
        }

        private fun calculateStatusCounts(storedResults: List<BlobStatus>): Status {
            val statusGroups = storedResults.mapNotNull { it.status }.groupingBy { status ->
                when (status) {
                    BlobStatus.Status.Succeeded -> "succeeded"
                    BlobStatus.Status.Failed -> "failed"
                    BlobStatus.Status.InProgress, BlobStatus.Status.Queued -> "inProgress"
                    BlobStatus.Status.Canceled -> "canceled"
                }
            }.eachCount()

            return Status(
                succeeded = statusGroups["succeeded"] ?: 0,
                failed = statusGroups["failed"] ?: 0,
                inProgress = statusGroups["inProgress"] ?: 0,
                canceled = statusGroups["canceled"] ?: 0
            )
        }
    }

    /**
     * Checks if this operation has any failed results.
     */
    val hasFailed: Boolean
        get() = storedResults.any { it.status == BlobStatus.Status.Failed }

    /**
     * Checks if this operation has completed (either succeeded or failed).
     */
    val isCompleted: Boolean
        get() = storedResults.isNotEmpty() && storedResults.all { result ->
            result.status in listOf(
                BlobStatus.Status.Succeeded,
                BlobStatus.Status.Failed,
                BlobStatus.Status.Canceled
            )
        }

    /**
     * Checks if this operation is still in progress.
     */
    val isInProgress: Boolean
        get() = storedResults.any { result ->
            result.status in listOf(
                BlobStatus.Status.Queued,
                BlobStatus.Status.InProgress
            )
        }

    /**
     * Gets all failed results from this operation.
     */
    val failedResults: List<BlobStatus>
        get() = storedResults.filter { it.status == BlobStatus.Status.Failed }

    /**
     * Gets all succeeded results from this operation.
     */
    val succeededResults: List<BlobStatus>
        get() = storedResults.filter { it.status == BlobStatus.Status.Succeeded }

    override fun toString(): String {
        return "IngestionOperation(id='$id', database='$database', table='$table', method=$ingestionMethod, isDataManagementOperationId=$isDataManagementOperationId, resultsCount=${storedResults.size})"
    }
}
