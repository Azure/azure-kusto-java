// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader.compression

/**
 * Exception thrown when compression operations fail.
 *
 * @param message The detail message
 * @param cause The underlying cause of the compression failure
 */
class CompressionException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)
