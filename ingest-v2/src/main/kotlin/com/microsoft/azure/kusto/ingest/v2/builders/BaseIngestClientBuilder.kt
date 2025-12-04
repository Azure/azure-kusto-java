// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.ClientDetails

abstract class BaseIngestClientBuilder<T : BaseIngestClientBuilder<T>> {
    protected var tokenCredential: TokenCredential? = null
    protected var skipSecurityChecks: Boolean = false
    protected var clientDetails: ClientDetails? = null

    protected abstract fun self(): T

    fun withAuthentication(credential: TokenCredential): T {
        this.tokenCredential = credential
        // Something to ponder about in AadAzureCoreTokenCredentialProvider. This has caching and
        // return as well
        return self()
    }

    fun skipSecurityChecks(): T {
        this.skipSecurityChecks = true
        return self()
    }

    /**
     * Sets the client details for tracing purposes, using defaults for any
     * unprovided fields.
     *
     * @param applicationName The client application name
     * @param version The client application version
     * @param userName The username for tracing (optional). If null, system
     *   default will be used.
     * @return This builder instance for method chaining
     */
    fun withClientDetails(
        applicationName: String,
        version: String,
        userName: String? = null,
    ): T {
        this.clientDetails =
            ClientDetails(
                applicationForTracing = applicationName,
                userNameForTracing = userName,
                clientVersionForTracing = version,
            )
        return self()
    }

    /**
     * Sets the client details with the format for connectors. Example output:
     * "Kusto.MyConnector:{1.0.0}|App.{MyApp}:{0.5.3}|CustomField:{CustomValue}"
     *
     * This method is useful for connectors and frameworks that want to identify
     * themselves in Kusto tracing with additional metadata.
     *
     * @param name Name of the connector (will be prefixed with "Kusto.")
     * @param version Version of the connector
     * @param sendUser Whether to send the username in tracing (default: false)
     * @param overrideUser Override username (used when sendUser is true)
     * @param appName Name of the application (optional, defaults to process
     *   name)
     * @param appVersion Version of the application (optional)
     * @param additionalFields Additional key-value pairs to include in the
     *   connector details
     * @return This builder instance for method chaining
     */
    fun withConnectorClientDetails(
        name: String,
        version: String,
        sendUser: Boolean = false,
        overrideUser: String? = null,
        appName: String? = null,
        appVersion: String? = null,
        additionalFields: Map<String, String>? = null,
    ): T {
        this.clientDetails =
            ClientDetails.fromConnectorDetails(
                name = name,
                version = version,
                sendUser = sendUser,
                overrideUser = overrideUser,
                appName = appName,
                appVersion = appVersion,
                additionalFields = additionalFields,
            )
        return self()
    }
}
