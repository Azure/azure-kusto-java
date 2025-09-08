package com.microsoft.azure.kusto.ingest.v2.common.auth

import com.azure.core.credential.TokenRequestContext
import com.azure.identity.AzureCliCredentialBuilder
import com.microsoft.azure.kusto.ingest.v2.common.models.KustoTokenCredentials

class AzCliTokenCredentialsProvider: TokenCredentialsProvider {
    override suspend fun getCredentialsAsync(targetResource: String):KustoTokenCredentials  {
        val azureCliCredential = AzureCliCredentialBuilder().build()
        val tokenRequestContext = TokenRequestContext().addScopes("$targetResource/.default")
        val token = azureCliCredential.getToken(tokenRequestContext).block()?.token
        val expiresOn = azureCliCredential.getToken(tokenRequestContext).block()?.expiresAt
        return KustoTokenCredentials("JWT",token ?: throw Exception("Failed to acquire token"), expiresOn)
    }

    override suspend fun getCredentialsAsync(targetResource: String, tenantId: String):KustoTokenCredentials {
        val azureCliCredential = AzureCliCredentialBuilder().tenantId(tenantId).build()
        val tokenRequestContext = TokenRequestContext().setTenantId(tenantId).addScopes("$targetResource/.default")
        val token = azureCliCredential.getToken(tokenRequestContext).block()?.token
        val expiresOn = azureCliCredential.getToken(tokenRequestContext).block()?.expiresAt
        return KustoTokenCredentials("JWT",token ?: throw Exception("Failed to acquire token"), expiresOn)

    }

    override suspend fun getCredentialsAsync(targetResource: String, retries: Int, tenantId: String?) :KustoTokenCredentials {
        //TODO: implement retries
        return getCredentialsAsync(targetResource, tenantId ?: "")
    }
}