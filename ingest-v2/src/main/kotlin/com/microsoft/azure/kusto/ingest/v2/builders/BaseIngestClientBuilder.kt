// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.KustoBaseApiClient
import com.microsoft.azure.kusto.ingest.v2.common.ClientDetails

abstract class BaseIngestClientBuilder<T : BaseIngestClientBuilder<T>> {
    protected var tokenCredential: TokenCredential? = null
    protected var skipSecurityChecks: Boolean = false
    protected var clientDetails: ClientDetails? = null

    // Added properties for ingestion endpoint and authentication
    protected var ingestionEndpoint: String? = null
    protected var clusterEndpoint: String? = null
    protected var authentication: TokenCredential? = null

    protected abstract fun self(): T

    fun withAuthentication(credential: TokenCredential): T {
        this.tokenCredential = credential
        this.authentication = credential // Set authentication
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

    protected fun createApiClient(
        dmUrl: String,
        tokenCredential: TokenCredential,
        clientDetails: ClientDetails,
        skipSecurityChecks: Boolean,
    ): KustoBaseApiClient {
        return KustoBaseApiClient(
            dmUrl = dmUrl,
            tokenCredential = tokenCredential,
            skipSecurityChecks = skipSecurityChecks,
            clientDetails = clientDetails,
        )
    }

    protected fun setEndpoint(endpoint: String) {
        this.ingestionEndpoint = normalizeAndCheckDmUrl(endpoint)
        this.clusterEndpoint = normalizeAndCheckEngineUrl(endpoint)
    }

    protected fun normalizeAndCheckEngineUrl(clusterUrl: String): String {
        val normalizedUrl =
            if (clusterUrl.matches(Regex("https://ingest-[^/]+.*"))) {
                // If the URL starts with https://ingest-, remove ingest-
                clusterUrl.replace(
                    Regex("https://ingest-([^/]+)"),
                    "https://$1",
                )
            } else {
                clusterUrl
            }
        return normalizedUrl
    }

    protected fun normalizeAndCheckDmUrl(dmUrl: String): String {
        val normalizedUrl =
            if (dmUrl.matches(Regex("https://(?!ingest-)[^/]+.*"))) {
                // If the URL starts with https:// and does not already have ingest-, add it
                dmUrl.replace(Regex("https://([^/]+)"), "https://ingest-$1")
            } else {
                dmUrl
            }
        return normalizedUrl
    }
}
