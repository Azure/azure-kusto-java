// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

import java.util.concurrent.ConcurrentHashMap

data class ClientDetails(
    val applicationForTracing: String?,
    val userNameForTracing: String?,
    val clientVersionForTracing: String?,
) {
    /**
     * Returns the application name for tracing, falling back to the process
     * name if not provided.
     */
    val effectiveApplicationForTracing: String
        get() = applicationForTracing ?: getProcessName()

    /**
     * Returns the user name for tracing, falling back to the system user if
     * not provided.
     */
    val effectiveUserNameForTracing: String
        get() = userNameForTracing ?: getUserName()

    /**
     * Returns the client version string for tracing, always including the
     * default SDK version and appending any custom version if provided.
     */
    val effectiveClientVersionForTracing: String
        get() {
            val defaultVersion = getDefaultVersion()
            return if (clientVersionForTracing != null) {
                "$defaultVersion|$clientVersionForTracing"
            } else {
                defaultVersion
            }
        }

    companion object {
        const val NONE = "[none]"
        const val DEFAULT_APP_NAME = "Kusto.Java.Client.V2"

        // Cache for default values to avoid recomputing on every call
        private val defaultValuesCache = ConcurrentHashMap<String, String>()

        /**
         * Escapes special characters in header field values by wrapping in
         * curly braces and replacing problematic characters with underscores.
         */
        private fun escapeField(field: String): String {
            val escaped = field.replace(Regex("[\\r\\n\\s{}|]+"), "_")
            return "{$escaped}"
        }

        /**
         * Formats the given fields into a string that can be used as a header.
         * Format: "field1:{value1}|field2:{value2}"
         */
        private fun formatHeader(args: Map<String, String>): String {
            return args.entries
                .filter { it.key.isNotBlank() && it.value.isNotBlank() }
                .joinToString("|") { (key, value) ->
                    "$key:${escapeField(value)}"
                }
        }

        /** Gets the process name from system properties with caching. */
        private fun getProcessName(): String {
            return defaultValuesCache.computeIfAbsent("processName") {
                val command = System.getProperty("sun.java.command")
                if (!command.isNullOrBlank()) {
                    // Strip file name from command line (matches
                    // UriUtils.stripFileNameFromCommandLine)
                    try {
                        val processName = command.trim().split(" ")[0]
                        java.nio.file.Paths.get(processName).fileName.toString()
                    } catch (_: Exception) {
                        "JavaProcess"
                    }
                } else {
                    "JavaProcess"
                }
            }
        }

        private fun getUserName(): String {
            return defaultValuesCache.computeIfAbsent("userName") {
                var user = System.getProperty("user.name")
                if (user.isNullOrBlank()) {
                    user = System.getenv("USERNAME")
                    val domain = System.getenv("USERDOMAIN")
                    if (!domain.isNullOrBlank() && !user.isNullOrBlank()) {
                        user = "$domain\\$user"
                    }
                }
                if (!user.isNullOrBlank()) user else NONE
            }
        }

        private fun getRuntime(): String {
            return defaultValuesCache.computeIfAbsent("runtime") {
                System.getProperty("java.runtime.name")
                    ?: System.getProperty("java.vm.name")
                    ?: System.getProperty("java.vendor")
                    ?: "UnknownRuntime"
            }
        }

        private fun getJavaVersion(): String {
            return defaultValuesCache.computeIfAbsent("javaVersion") {
                System.getProperty("java.version") ?: "UnknownVersion"
            }
        }

        /**
         * Gets the default client version string with caching. Format:
         * "Kusto.Java.Client:{version}|Runtime.{runtime}:{javaVersion}"
         */
        private fun getDefaultVersion(): String {
            return defaultValuesCache.computeIfAbsent("defaultVersion") {
                val baseMap =
                    linkedMapOf(
                        DEFAULT_APP_NAME to getPackageVersion(),
                        "Runtime.${escapeField(getRuntime())}" to
                            getJavaVersion(),
                    )
                formatHeader(baseMap)
            }
        }

        /** Gets the package version from the manifest or returns a default. */
        private fun getPackageVersion(): String {
            return try {
                val props = java.util.Properties()
                ClientDetails::class
                    .java
                    .getResourceAsStream("/app.properties")
                    ?.use { stream ->
                        props.load(stream)
                        props.getProperty("version")?.trim() ?: ""
                    } ?: ""
            } catch (_: Exception) {
                ""
            }
        }

        /**
         * Creates a ClientDetails from connector details Example output:
         * "Kusto.MyConnector:{1.0.0}|App.{MyApp}:{0.5.3}|CustomField:{CustomValue}"
         *
         * @param name The name of the connector (will be prefixed with
         *   "Kusto.")
         * @param version The version of the connector
         * @param sendUser True if the user should be sent to Kusto, otherwise
         *   "[none]" will be sent
         * @param overrideUser The user to send to Kusto, or null to use the
         *   current user
         * @param appName The app hosting the connector, or null to use the
         *   current process name
         * @param appVersion The version of the app hosting the connector, or
         *   null to use "[none]"
         * @param additionalFields Additional fields to trace as key-value pairs
         * @return ClientDetails instance with formatted connector information
         */
        fun fromConnectorDetails(
            name: String,
            version: String,
            sendUser: Boolean = false,
            overrideUser: String? = null,
            appName: String? = null,
            appVersion: String? = null,
            additionalFields: Map<String, String>? = null,
        ): ClientDetails {
            val fieldsMap = linkedMapOf<String, String>()
            fieldsMap["Kusto.$name"] = version

            val finalAppName = appName ?: getProcessName()
            val finalAppVersion = appVersion ?: NONE
            fieldsMap["App.${escapeField(finalAppName)}"] = finalAppVersion

            additionalFields?.let { fieldsMap.putAll(it) }

            val app = formatHeader(fieldsMap)

            val user =
                if (sendUser) {
                    overrideUser ?: getUserName()
                } else {
                    NONE
                }

            return ClientDetails(app, user, null)
        }

        fun createDefault(): ClientDetails {
            return ClientDetails(
                applicationForTracing = getProcessName(),
                userNameForTracing = getUserName(),
                clientVersionForTracing = null,
            )
        }
    }

}
