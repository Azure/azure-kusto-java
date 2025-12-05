// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.exceptions

import com.microsoft.azure.kusto.ingest.v2.container.UploadErrorCode

open class IngestException(
    message: String? = null,
    cause: Throwable? = null,
    val failureCode: Int? = null,
    val failureSubCode: String? = null,
    val isPermanent: Boolean? = null,
) : Exception(message, cause) {
    open val alreadyTraced: Boolean = false
    open val creationMessage: String? = message

    override val message: String
        get() =
            creationMessage
                ?: "Something went wrong calling Kusto client library (fallback message)."

    override fun toString(): String = message
}

class IngestRequestException(
    val errorCode: String? = null,
    val errorReason: String? = null,
    val errorMessage: String? = null,
    val dataSource: String? = null,
    val databaseName: String? = null,
    val clientRequestId: String? = null,
    val activityId: String? = null,
    failureCode: Int? = null,
    failureSubCode: String? = null,
    isPermanent: Boolean? = true,
    message: String? = null,
    cause: Throwable? = null,
) :
    IngestException(
        message,
        cause,
        failureCode,
        failureSubCode.toString(),
        isPermanent,
    ) {
    override val message: String
        get() =
            creationMessage
                ?: "${errorReason ?: ""} (${errorCode ?: ""}): ${errorMessage ?: ""}. This normally represents a permanent error, and retrying is unlikely to help."
}

class IngestServiceException(
    val errorCode: String? = null,
    val errorReason: String? = null,
    val errorMessage: String? = null,
    val dataSource: String? = null,
    val clientRequestId: String? = null,
    val activityId: String? = null,
    failureCode: Int? = 500,
    failureSubCode: String? = null,
    isPermanent: Boolean? = null,
    message: String? = null,
    cause: Throwable? = null,
) : IngestException(message, cause, failureCode, failureSubCode, isPermanent) {
    override val message: String
        get() =
            creationMessage
                ?: "${errorReason ?: ""} (${errorCode ?: ""}): ${errorMessage ?: ""}. This normally represents a temporary error, and retrying after some backoff period might help."
}

open class IngestClientException(
    val ingestionSourceId: String? = null,
    val ingestionSource: String? = null,
    val error: String? = null,
    failureCode: Int? = 400,
    failureSubCode: String? = null,
    isPermanent: Boolean? = null,
    message: String? = null,
    cause: Throwable? = null,
) : IngestException(message, cause, failureCode, failureSubCode, isPermanent) {
    override val message: String
        get() =
            creationMessage
                ?: "An error occurred for source: '${ingestionSource ?: ""}'. Error: '${error ?: ""}'"
}

class IngestSizeLimitExceededException(
    val size: Long,
    val maxNumberOfBlobs: Int,
    ingestionSourceId: String? = null,
    ingestionSource: String? = null,
    error: String? = null,
    failureCode: Int? = 400,
    failureSubCode: String? = null,
    isPermanent: Boolean? = true,
    message: String? = null,
    cause: Throwable? = null,
) :
    IngestClientException(
        ingestionSourceId,
        ingestionSource,
        error,
        failureCode,
        failureSubCode,
        isPermanent,
        message,
        cause,
    ) {
    override val message: String
        get() =
            creationMessage
                ?: "Size too large to ingest: Source: '${ingestionSource ?: ""}' size in bytes is '$size' which exceeds the maximal size of '$maxNumberOfBlobs'"
}

class InvalidIngestionMappingException(
    ingestionSourceId: String? = null,
    ingestionSource: String? = null,
    error: String? = null,
    failureCode: Int? = 400,
    failureSubCode: String? = null,
    isPermanent: Boolean? = true,
    message: String? = null,
    cause: Throwable? = null,
) :
    IngestClientException(
        ingestionSourceId,
        ingestionSource,
        error,
        failureCode,
        failureSubCode,
        isPermanent,
        message,
        cause,
    ) {
    override val message: String
        get() =
            creationMessage
                ?: "Ingestion mapping is invalid: ${super.message ?: ""}"
}

class MultipleIngestionMappingPropertiesException(
    ingestionSourceId: String? = null,
    ingestionSource: String? = null,
    error: String? = null,
    failureCode: Int? = 400,
    failureSubCode: String? = null,
    isPermanent: Boolean? = true,
    message: String? = null,
    cause: Throwable? = null,
) :
    IngestClientException(
        ingestionSourceId,
        ingestionSource,
        error,
        failureCode,
        failureSubCode,
        isPermanent,
        message,
        cause,
    ) {
    override val message: String
        get() =
            creationMessage
                ?: "At most one property of type ingestion mapping or ingestion mapping reference must be present."
}

open class UploadFailedException(
    val fileName: String? = null,
    val blobName: String? = null,
    failureCode: Int? = null,
    failureSubCode: UploadErrorCode,
    isPermanent: Boolean? = null,
    message: String? = null,
    cause: Throwable? = null,
) :
    IngestException(
        message,
        cause,
        failureCode,
        failureSubCode.toString(),
        isPermanent,
    ) {
    override val message: String
        get() =
            creationMessage
                ?: "An error occurred while attempting to upload file `$fileName` to blob `$blobName`."
}

class NoAvailableIngestContainersException(
    fileName: String? = null,
    blobName: String? = null,
    failureCode: Int? = 500,
    failureSubCode: UploadErrorCode,
    isPermanent: Boolean? = false,
    message: String? = null,
    cause: Throwable? = null,
) :
    UploadFailedException(
        fileName,
        blobName,
        failureCode,
        failureSubCode,
        isPermanent,
        message,
        cause,
    ) {
    override val message: String
        get() = creationMessage ?: "No available containers for upload."
}

class InvalidUploadStreamException(
    fileName: String? = null,
    blobName: String? = null,
    failureCode: Int? = null,
    failureSubCode: UploadErrorCode,
    isPermanent: Boolean? = true,
    message: String? = null,
    cause: Throwable? = null,
) :
    UploadFailedException(
        fileName,
        blobName,
        failureCode,
        failureSubCode,
        isPermanent,
        message,
        cause,
    ) {
    override val message: String
        get() =
            creationMessage
                ?: "The stream provided for upload is invalid - $failureSubCode."
}

class UploadSizeLimitExceededException(
    val size: Long,
    val maxSize: Long,
    fileName: String? = null,
    blobName: String? = null,
    failureCode: Int? = null,
    failureSubCode: UploadErrorCode,
    isPermanent: Boolean? = true,
    message: String? = null,
    cause: Throwable? = null,
) :
    UploadFailedException(
        fileName,
        blobName,
        failureCode,
        failureSubCode,
        isPermanent,
        message,
        cause,
    ) {
    override val message: String
        get() =
            creationMessage
                ?: "The file `$fileName` is too large to upload. Size: $size bytes, Max size: $maxSize bytes."
}
