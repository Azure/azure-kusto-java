// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.auth.endpoints

import com.microsoft.azure.kusto.ingest.v2.exceptions.KustoClientInvalidConnectionStringException
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.URISyntaxException

/**
 * A helper class to determine which DNS names are "well-known/trusted" Kusto
 * endpoints. Untrusted endpoints might require additional configuration before
 * they can be used, for security reasons.
 */
object KustoTrustedEndpoints {
    private val logger =
        LoggerFactory.getLogger(KustoTrustedEndpoints::class.java)

    /**
     * Global flag to enable/disable endpoint validation. When false, untrusted
     * endpoints will only log a warning instead of throwing an exception.
     */
    @JvmField
    @Volatile
    var enableWellKnownKustoEndpointsValidation: Boolean = true

    private val matchers: MutableMap<String, FastSuffixMatcher> = mutableMapOf()

    @Volatile private var additionalMatcher: FastSuffixMatcher? = null

    @Volatile private var overrideMatcher: ((String) -> Boolean)? = null

    // Default login endpoint for public cloud
    private const val DEFAULT_PUBLIC_LOGIN_ENDPOINT =
        "https://login.microsoftonline.com"

    init {
        loadEndpointsFromJson()
    }

    private fun loadEndpointsFromJson() {
        try {
            val endpointsData = WellKnownKustoEndpointsData.getInstance()

            endpointsData.allowedEndpointsByLogin.forEach {
                    (loginEndpoint, allowedEndpoints) ->
                val rules = mutableListOf<MatchRule>()

                // Add suffix rules (exact = false)
                allowedEndpoints.allowedKustoSuffixes.forEach { suffix ->
                    rules.add(MatchRule(suffix, exact = false))
                }

                // Add hostname rules (exact = true)
                allowedEndpoints.allowedKustoHostnames.forEach { hostname ->
                    rules.add(MatchRule(hostname, exact = true))
                }

                if (rules.isNotEmpty()) {
                    matchers[loginEndpoint.lowercase()] =
                        FastSuffixMatcher.create(rules)
                }
            }

            logger.debug(
                "Loaded {} login endpoint configurations from WellKnownKustoEndpoints.json",
                matchers.size,
            )
        } catch (ex: Exception) {
            logger.error("Failed to load WellKnownKustoEndpoints.json", ex)
            throw ex
        }
    }

    /**
     * Sets an override policy for endpoint validation.
     *
     * @param matcher Rules that determine if a hostname is a valid/trusted
     *   Kusto endpoint (replaces existing rules)
     */
    fun setOverridePolicy(matcher: ((String) -> Boolean)?) {
        overrideMatcher = matcher
    }

    /**
     * Adds additional trusted hosts to the matcher.
     *
     * @param rules A set of rules
     * @param replace If true, nullifies the last added rules
     */
    fun addTrustedHosts(rules: List<MatchRule>?, replace: Boolean) {
        if (rules.isNullOrEmpty()) {
            if (replace) {
                additionalMatcher = null
            }
            return
        }

        additionalMatcher =
            FastSuffixMatcher.create(
                if (replace) null else additionalMatcher,
                rules,
            )
    }

    /**
     * Validates that the endpoint is trusted.
     *
     * @param uri Kusto endpoint URI string
     * @param loginEndpoint The login endpoint to check against (optional,
     *   defaults to public cloud)
     * @throws KustoClientInvalidConnectionStringException if endpoint is not
     *   trusted
     */
    fun validateTrustedEndpoint(
        uri: String,
        loginEndpoint: String = DEFAULT_PUBLIC_LOGIN_ENDPOINT,
    ) {
        try {
            validateTrustedEndpoint(URI(uri), loginEndpoint)
        } catch (ex: URISyntaxException) {
            throw KustoClientInvalidConnectionStringException(
                uri,
                ex.message ?: "Invalid URI",
                ex,
            )
        }
    }

    /**
     * Validates that the endpoint is trusted.
     *
     * @param uri Kusto endpoint URI
     * @param loginEndpoint The login endpoint to check against
     * @throws KustoClientInvalidConnectionStringException if endpoint is not
     *   trusted
     */
    fun validateTrustedEndpoint(uri: URI, loginEndpoint: String) {
        val host = uri.host ?: uri.toString()
        validateHostnameIsTrusted(host, loginEndpoint)
    }

    /**
     * Validates that a hostname is trusted.
     *
     * @param hostname The hostname to validate
     * @param loginEndpoint The login endpoint to check against
     * @throws KustoClientInvalidConnectionStringException if hostname is not
     *   trusted
     */
    private fun validateHostnameIsTrusted(
        hostname: String,
        loginEndpoint: String,
    ) {
        // Loopback addresses are unconditionally allowed (we trust ourselves)
        if (isLocalAddress(hostname)) {
            return
        }

        // Check override matcher first
        val override = overrideMatcher
        if (override != null) {
            if (override(hostname)) {
                return
            }
        } else {
            // Check against login-specific matchers
            val matcher = matchers[loginEndpoint.lowercase()]
            if (matcher != null && matcher.isMatch(hostname)) {
                return
            }
        }

        // Check additional matchers
        val additional = additionalMatcher
        if (additional != null && additional.isMatch(hostname)) {
            return
        }

        // Not trusted
        if (!enableWellKnownKustoEndpointsValidation) {
            logger.warn(
                "Can't communicate with '{}' as this hostname is currently not trusted; " +
                    "please see https://aka.ms/kustotrustedendpoints.",
                hostname,
            )
            return
        }

        throw KustoClientInvalidConnectionStringException(
            "\$\$ALERT[ValidateHostnameIsTrusted]: Can't communicate with '$hostname' " +
                "as this hostname is currently not trusted; " +
                "please see https://aka.ms/kustotrustedendpoints",
        )
    }

    /** Checks if the hostname is a local/loopback address. */
    private fun isLocalAddress(hostname: String): Boolean {
        val lowerHost = hostname.lowercase()
        return lowerHost == "localhost" ||
            lowerHost == "127.0.0.1" ||
            lowerHost == "::1" ||
            lowerHost == "[::1]" ||
            lowerHost.startsWith("localhost:")
    }

    /**
     * Checks if a hostname is trusted without throwing an exception.
     *
     * @param hostname The hostname to check
     * @param loginEndpoint The login endpoint to check against
     * @return true if the hostname is trusted
     */
    fun isTrusted(
        hostname: String,
        loginEndpoint: String = DEFAULT_PUBLIC_LOGIN_ENDPOINT,
    ): Boolean {
        return try {
            validateHostnameIsTrusted(hostname, loginEndpoint)
            true
        } catch (_: KustoClientInvalidConnectionStringException) {
            false
        }
    }
}
