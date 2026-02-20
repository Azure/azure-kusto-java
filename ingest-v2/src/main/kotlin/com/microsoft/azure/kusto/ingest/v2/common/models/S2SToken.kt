// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

/**
 * Represents an S2S (Service-to-Service) authentication token used for Fabric
 * Private Link authentication.
 *
 * @property scheme The authentication scheme (e.g., "Bearer")
 * @property token The authentication token value
 */
data class S2SToken(val scheme: String, val token: String) {
    /**
     * Formats the token as an HTTP Authorization header value.
     *
     * @return The formatted header value in the format "{scheme} {token}"
     */
    fun toHeaderValue(): String = "$scheme $token"

    companion object {
        /**
         * Creates an S2SToken with the Bearer scheme.
         *
         * @param token The token value
         * @return An S2SToken with scheme "Bearer"
         */
        fun bearer(token: String): S2SToken = S2SToken("Bearer", token)
    }
}
