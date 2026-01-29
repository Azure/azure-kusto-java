// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.builders

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.IngestClientBase
import com.microsoft.azure.kusto.ingest.v2.KustoBaseApiClient
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_CONCURRENCY
import com.microsoft.azure.kusto.ingest.v2.UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.models.S2SToken
import com.microsoft.azure.kusto.ingest.v2.uploader.IUploader
import com.microsoft.azure.kusto.ingest.v2.uploader.ManagedUploader

abstract class BaseIngestClientBuilder<T : BaseIngestClientBuilder<T>> {
    protected var tokenCredential: TokenCredential? = null
    protected var skipSecurityChecks: Boolean = false
    protected var clientDetails: ClientDetails? = null

    // Fabric Private Link support
    protected var s2sTokenProvider: (suspend () -> S2SToken)? = null
    protected var s2sFabricPrivateLinkAccessContext: String? = null

    protected var maxConcurrency: Int = UPLOAD_CONTAINER_MAX_CONCURRENCY
    protected var maxDataSize: Long = UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES
    protected var ignoreFileSize: Boolean = false
    protected var uploader: IUploader? = null
    protected var closeUploader: Boolean = false
    protected var configuration: ConfigurationCache? = null

    protected abstract fun self(): T

    fun withAuthentication(credential: TokenCredential): T {
        this.tokenCredential = credential
        return self()
    }

    fun skipSecurityChecks(): T {
        this.skipSecurityChecks = true
        return self()
    }

    /**
     * Enables a run request to target a cluster with Fabric Private Link
     * enabled.
     *
     * @param s2sTokenProvider A suspend function that provides the S2S
     *   (Service-to-Service) token, indicating that the caller is authorized as
     *   a valid Fabric Private Link client. Returns a Pair of (token, scheme)
     *   e.g., ("token_value", "Bearer") Note: The header format will be
     *   "{scheme} {token}" (scheme first)
     * @param s2sFabricPrivateLinkAccessContext Specifies the scope of the
     *   Fabric Private Link perimeter, such as the entire tenant or a specific
     *   workspace.
     * @return This builder instance for method chaining
     */
    fun withFabricPrivateLink(
        s2sTokenProvider: suspend () -> S2SToken,
        s2sFabricPrivateLinkAccessContext: String,
    ): T {
        require(s2sFabricPrivateLinkAccessContext.isNotBlank()) {
            "s2sFabricPrivateLinkAccessContext must not be blank"
        }
        this.s2sTokenProvider = s2sTokenProvider
        this.s2sFabricPrivateLinkAccessContext =
            s2sFabricPrivateLinkAccessContext
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
            s2sTokenProvider = s2sTokenProvider,
            s2sFabricPrivateLinkAccessContext =
            s2sFabricPrivateLinkAccessContext,
        )
    }

    protected fun createDefaultUploader(
        configuration: ConfigurationCache,
        ignoreFileSize: Boolean,
        maxConcurrency: Int,
        maxDataSize: Long,
    ): IUploader {
        return ManagedUploader.builder()
            .withConfigurationCache(configuration)
            .withIgnoreSizeLimit(ignoreFileSize)
            .withMaxConcurrency(maxConcurrency)
            .withMaxDataSize(maxDataSize)
            .apply { tokenCredential?.let { withTokenCredential(it) } }
            .build()
    }

    companion object {
        /**
         * Converts an ingestion endpoint URL to a query/engine endpoint URL by
         * removing the "ingest-" prefix.
         *
         * Special URLs (localhost, IP addresses, onebox.dev.kusto.windows.net)
         * are returned unchanged.
         *
         * @param clusterUrl The ingestion endpoint URL to convert
         * @return The query endpoint URL without "ingest-" prefix
         */
        protected fun normalizeAndCheckEngineUrl(clusterUrl: String): String {
            return IngestClientBase.getQueryEndpoint(clusterUrl) ?: clusterUrl
        }

        /**
         * Converts a cluster URL to an ingestion endpoint URL by adding the
         * "ingest-" prefix.
         *
         * Special URLs (localhost, IP addresses, onebox.dev.kusto.windows.net)
         * are returned unchanged to support local development and testing.
         *
         * @param dmUrl The cluster URL to convert
         * @return The ingestion endpoint URL with "ingest-" prefix
         */
        @JvmStatic
        protected fun normalizeAndCheckDmUrl(dmUrl: String): String {
            return IngestClientBase.getIngestionEndpoint(dmUrl) ?: dmUrl
        }
    }
}
