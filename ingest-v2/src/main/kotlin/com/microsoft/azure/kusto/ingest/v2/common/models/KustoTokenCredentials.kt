/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2.common.models

import java.time.OffsetDateTime

/** Represents a token credentials holder, capable (at least) of authenticating over an HTTPS "Authorization" header. */
data class KustoTokenCredentials(
    val tokenScheme: String? = null,
    val tokenValue: String? = null,
    val expiresOn: OffsetDateTime? = null,
) {
    /** Returns the secure representation of this instance. */
    fun toSecureString(): String {
        return "${this::class.simpleName}:$tokenScheme:*****"
    }

    override fun toString(): String = toSecureString()
}
