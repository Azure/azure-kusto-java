// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader.models

enum class UploadErrorCode(val code: String, val description: String) {
    // Stream validation errors
    SOURCE_IS_NULL("UploadError_SourceIsNull", "Upload source is null"),
    SOURCE_NOT_FOUND("UploadError_SourceNotFound", "Upload source not found"),
    SOURCE_NOT_READABLE(
        "UploadError_SourceNotReadable",
        "Upload source is not readable",
    ),
    SOURCE_IS_EMPTY("UploadError_SourceIsEmpty", "Upload source is empty"),

    // Size validation errors
    SOURCE_SIZE_LIMIT_EXCEEDED(
        "UploadError_SourceSizeLimitExceeded",
        "Upload source exceeds maximum allowed size",
    ),

    // Upload errors
    UPLOAD_FAILED("UploadError_Failed", "Upload operation failed"),
    NO_CONTAINERS_AVAILABLE(
        "UploadError_NoContainersAvailable",
        "No upload containers available",
    ),
    CONTAINER_UNAVAILABLE(
        "UploadError_ContainerUnavailable",
        "Upload container is unavailable",
    ),

    // Network/Azure errors
    NETWORK_ERROR("UploadError_NetworkError", "Network error during upload"),
    AUTHENTICATION_FAILED(
        "UploadError_AuthenticationFailed",
        "Authentication failed for upload",
    ),

    // General
    UNKNOWN("UploadError_Unknown", "Unknown upload error"),
    ;

    override fun toString(): String = code
}
