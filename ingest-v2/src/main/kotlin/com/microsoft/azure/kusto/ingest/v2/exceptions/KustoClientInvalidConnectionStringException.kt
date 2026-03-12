// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.exceptions

/**
 * Exception thrown when a connection string is invalid or the endpoint is not
 * trusted.
 */
class KustoClientInvalidConnectionStringException : RuntimeException {
    constructor(message: String) : super(message)

    constructor(
        uri: String,
        message: String,
        cause: Throwable,
    ) : super("Invalid connection string for URI '$uri': $message", cause)
}
