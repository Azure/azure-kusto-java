// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.StreamingIngestClient
import com.microsoft.azure.kusto.ingest.v2.common.ClientDetails

/**
 * Builder for creating [StreamingIngestClient] instances with a fluent API.
 * This builder provides a more discoverable and extensible way to configure
 * ingestion clients compared to direct constructor calls.
 *
 * @property engineUrl The cluster engine endpoint URL
 */
class StreamingIngestClientBuilder private constructor(
    private val engineUrl: String,
) {
    private var tokenCredential: TokenCredential? = null
    private var skipSecurityChecks: Boolean = false
    private var clientDetails: ClientDetails? = null

    companion object {
        @JvmStatic
        fun create(engineUrl: String): StreamingIngestClientBuilder {
            require(engineUrl.isNotBlank()) { "Engine URL cannot be blank" }
            return StreamingIngestClientBuilder(engineUrl)
        }
    }

    fun withAuthentication(credential: TokenCredential): StreamingIngestClientBuilder {
        this.tokenCredential = credential
        return this
    }

    fun skipSecurityChecks(): StreamingIngestClientBuilder {
        this.skipSecurityChecks = true
        return this
    }

    /**
     * Sets simple client details for tracing.
     *
     * @param name The client application name
     * @param version The client application version
     * @return This builder instance for method chaining
     */
    fun withClientDetails(
        name: String,
        version: String,
    ): StreamingIngestClientBuilder {
        this.clientDetails = ClientDetails(
            applicationForTracing = name,
            userNameForTracing = null,
            clientVersionForTracing = version
        )
        return this
    }

    /**
     * Sets the client details with the format for connectors, matching the existing Java implementation.
     * Example output: "Kusto.MyConnector:{1.0.0}|App.{MyApp}:{0.5.3}|CustomField:{CustomValue}"
     *
     * This method is useful for connectors and frameworks that want to identify themselves
     * in Kusto tracing with additional metadata.
     *
     * @param name Name of the connector (will be prefixed with "Kusto.")
     * @param version Version of the connector
     * @param sendUser Whether to send the username in tracing (default: false)
     * @param overrideUser Override username (used when sendUser is true)
     * @param appName Name of the application (optional, defaults to process name)
     * @param appVersion Version of the application (optional)
     * @param additionalFields Additional key-value pairs to include in the connector details
     * @return This builder instance for method chaining
     */
    fun withConnectorClientDetails(
        name: String,
        version: String,
        sendUser: Boolean = false,
        overrideUser: String? = null,
        appName: String? = null,
        appVersion: String? = null,
        additionalFields: Map<String, String>? = null
    ): StreamingIngestClientBuilder {
        this.clientDetails = ClientDetails.fromConnectorDetails(
            name = name,
            version = version,
            sendUser = sendUser,
            overrideUser = overrideUser,
            appName = appName,
            appVersion = appVersion,
            additionalFields = additionalFields
        )
        return this
    }

    fun build(): StreamingIngestClient {
        requireNotNull(tokenCredential) {
            "Authentication is required. Call withAuthentication() before build()"
        }


        // TODO: Future-proofing for additional parameters
        // Currently, only these parameters are supported by the client constructor
        // Future parameters (clientName, clientVersion) will be passed
        // when the client constructor is updated to support them
        return StreamingIngestClient(
            engineUrl = engineUrl,
            tokenCredential = tokenCredential!!,
            skipSecurityChecks = skipSecurityChecks,
            clientDetails = clientDetails,
        )
    }
}
