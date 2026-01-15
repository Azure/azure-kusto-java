// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Error response model for streaming ingestion failures. This represents the
 * error structure returned by Kusto when a streaming ingestion request fails.
 */
@Serializable
data class StreamingIngestionErrorResponse(
    @SerialName("error") val error: StreamingIngestionError,
)

@Serializable
data class StreamingIngestionError(
    @SerialName("code") val code: String? = null,
    @SerialName("message") val message: String? = null,
    @SerialName("@type") val type: String? = null,
    @SerialName("@message") val detailedMessage: String? = null,
    @SerialName("@failureCode") val failureCode: Int? = null,
    @SerialName("@context") val context: ErrorContext? = null,
    @SerialName("@permanent") val permanent: Boolean? = null,
)

@Serializable
data class ErrorContext(
    @SerialName("timestamp") val timestamp: String? = null,
    @SerialName("serviceAlias") val serviceAlias: String? = null,
    @SerialName("clientRequestId") val clientRequestId: String? = null,
    @SerialName("activityId") val activityId: String? = null,
    @SerialName("subActivityId") val subActivityId: String? = null,
    @SerialName("activityType") val activityType: String? = null,
    @SerialName("parentActivityId") val parentActivityId: String? = null,
    @SerialName("activityStack") val activityStack: String? = null,
    @SerialName("serviceFarm") val serviceFarm: String? = null,
)
