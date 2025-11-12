// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.QueuedIngestionClient
import com.microsoft.azure.kusto.ingest.v2.common.ClientDetails

/**
 * Builder for creating [QueuedIngestionClient] instances with a fluent API.
 * This provides a more discoverable and extensible way to configure
 * ingestion clients compared to direct constructor calls.
 *
 * @property dmUrl The data management endpoint URL
 */
class QueuedIngestionClientBuilder private constructor(
    private val dmUrl: String,
) {
    private var tokenCredential: TokenCredential? = null
    private var skipSecurityChecks: Boolean = false
    private var clientDetails: ClientDetails? = null

    // Future-ready optional parameters (currently not passed to client)
    private var maxConcurrency: Int? = null

    companion object {
        @JvmStatic
        fun create(dmUrl: String): QueuedIngestionClientBuilder {
            require(dmUrl.isNotBlank()) { "Data management URL cannot be blank" }
            return QueuedIngestionClientBuilder(dmUrl)
        }
    }

    fun withAuthentication(credential: TokenCredential): QueuedIngestionClientBuilder {
        this.tokenCredential = credential
        return this
    }

    fun skipSecurityChecks(): QueuedIngestionClientBuilder {
        this.skipSecurityChecks = true
        return this
    }

    /**
     * @param name The client application name
     * @param version The client application version
     * @return This builder instance for method chaining
     */
    fun withClientDetails(
        name: String,
        version: String,
    ): QueuedIngestionClientBuilder {
        this.clientDetails = ClientDetails(
            applicationForTracing = name,
            userNameForTracing = null,
            clientVersionForTracing = version
        )
        return this
    }

    /**
     * Example output: "Kusto.MyConnector:{1.0.0}|App.{MyApp}:{0.5.3}|CustomField:{CustomValue}"
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
    ): QueuedIngestionClientBuilder {
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

    /**
     *
     * @param concurrency The maximum number of concurrent uploads (must be positive)
     * @return This builder instance for method chaining
     */
    fun withMaxConcurrency(concurrency: Int): QueuedIngestionClientBuilder {
        require(concurrency > 0) { "Max concurrency must be positive, got: $concurrency" }
        this.maxConcurrency = concurrency
        return this
    }
    
    fun build(): QueuedIngestionClient {
        requireNotNull(tokenCredential) {
            "Authentication is required. Call withAuthentication() before build()"
        }

        // Currently, only these parameters are supported by the client constructor
        // Future parameters will be passed
        // when the client constructor is updated to support them
        return QueuedIngestionClient(
            dmUrl = dmUrl,
            tokenCredential = tokenCredential!!,
            skipSecurityChecks = skipSecurityChecks,
            clientDetails = clientDetails,
        )
    }
}
