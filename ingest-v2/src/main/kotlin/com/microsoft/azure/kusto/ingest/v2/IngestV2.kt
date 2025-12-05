// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

// Size of each block to upload to Azure Blob Storage (4 MB)
const val UPLOAD_BLOCK_SIZE_BYTES: Long = 4 * 1024 * 1024

// Maximum size for a single upload operation to Azure Blob Storage (256 MB)
const val UPLOAD_MAX_SINGLE_SIZE_BYTES: Long = 256 * 1024 * 1024

// Request timeout in milliseconds for Kusto API HTTP requests
const val KUSTO_API_REQUEST_TIMEOUT_MS: Long = 60_000

// Connection timeout in milliseconds for Kusto API HTTP requests
const val KUSTO_API_CONNECT_TIMEOUT_MS: Long = 60_000

// Socket timeout in milliseconds for Kusto API HTTP requests
const val KUSTO_API_SOCKET_TIMEOUT_MS: Long = 60_000

// Kusto API version used in HTTP requests
const val KUSTO_API_VERSION = "2024-12-12"

// Default refresh interval for configuration cache (1 hour)
const val CONFIG_CACHE_DEFAULT_REFRESH_INTERVAL_HOURS: Long = 1

// Default value for skipSecurityChecks if not provided
const val CONFIG_CACHE_DEFAULT_SKIP_SECURITY_CHECKS: Boolean = false

// Default interval between retries for SimpleRetryPolicy (10 seconds)
const val INGEST_RETRY_POLICY_DEFAULT_INTERVAL_SECONDS: Long = 10

// Default total number of retries for SimpleRetryPolicy
const val INGEST_RETRY_POLICY_DEFAULT_TOTAL_RETRIES: Int = 3

// Default timeout for blob upload operations (1 hour)
const val BLOB_UPLOAD_TIMEOUT_HOURS: Long = 1

// Default retry intervals for CustomRetryPolicy (1s, 3s, 7s)
val INGEST_RETRY_POLICY_CUSTOM_INTERVALS: Array<Long> = arrayOf(1, 3, 7)

// Number of blobs to upload in a single batch
const val MAX_BLOBS_PER_BATCH: Int = 4

// Default maximum data size for blob upload operations (4GB)
const val UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES: Long = 4L * 1024 * 1024 * 1024

// Default maximum concurrency for blob upload operations
const val UPLOAD_CONTAINER_MAX_CONCURRENCY: Int = 4

const val STREAMING_MAX_REQ_BODY_SIZE = 10 * 1024 * 1024 // 10 MB;
