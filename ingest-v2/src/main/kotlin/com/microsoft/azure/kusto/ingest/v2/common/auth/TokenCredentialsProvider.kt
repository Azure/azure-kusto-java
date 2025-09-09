// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.auth

import com.microsoft.azure.kusto.ingest.v2.common.models.KustoTokenCredentials

interface TokenCredentialsProvider {
    /**
     * Retrieves (or creates) a [KustoTokenCredentials] object for
     * [targetResource].
     *
     * @param targetResource The target resource for which the credentials are
     *   needed.
     * @return The [KustoTokenCredentials] concrete object to use when accessing
     *   the target resource.
     */
    suspend fun getCredentialsAsync(
        targetResource: String,
    ): KustoTokenCredentials

    /**
     * Retrieves (or creates) a [KustoTokenCredentials] object for the
     * [targetResource] on a tenant [tenantId]. Note this API is NOT always
     * supported. Make sure the implementation you use supports this API.
     */
    suspend fun getCredentialsAsync(
        targetResource: String,
        tenantId: String,
    ): KustoTokenCredentials

    /**
     * Retrieves (or creates) a [KustoTokenCredentials] object for the
     * [targetResource] on a tenant [tenantId] with retries. Note this API is
     * NOT always supported. Make sure the implementation you use supports this
     * API.
     */
    suspend fun getCredentialsAsync(
        targetResource: String,
        retries: Int,
        tenantId: String? = null,
    ): KustoTokenCredentials
}
