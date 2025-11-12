// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

/**
 * Contains information about the application/user/client that sends requests to Kusto.
 * This information is passed to the Kusto service for tracing purposes.
 */
data class ClientDetails(
    val applicationForTracing: String?,
    val userNameForTracing: String?,
    val clientVersionForTracing: String?,
) {
    companion object {
        const val NONE = "[none]"

        /**
         * Escapes special characters in header field values by wrapping in curly braces
         * and replacing problematic characters with underscores.
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

        /**
         * Gets the process name from system properties.
         */
        private fun getProcessName(): String {
            val command = System.getProperty("sun.java.command")
            return if (!command.isNullOrBlank()) {
                // Strip file name from command line (matches UriUtils.stripFileNameFromCommandLine)
                command.split(" ").firstOrNull() ?: "JavaProcess"
            } else {
                "JavaProcess"
            }
        }

        private fun getUserName(): String {
            var user = System.getProperty("user.name")
            if (user.isNullOrBlank()) {
                user = System.getenv("USERNAME")
                val domain = System.getenv("USERDOMAIN")
                if (!domain.isNullOrBlank() && !user.isNullOrBlank()) {
                    user = "$domain\\$user"
                }
            }
            return if (!user.isNullOrBlank()) user else NONE
        }

        private fun getRuntime(): String {
            return System.getProperty("java.runtime.name")
                ?: System.getProperty("java.vm.name")
                ?: System.getProperty("java.vendor")
                ?: "UnknownRuntime"
        }

        private fun getJavaVersion(): String {
            return System.getProperty("java.version") ?: "UnknownVersion"
        }

        /**
         * Gets the default client version string.
         * Format: "Kusto.Java.Client:{version}|Runtime.{runtime}:{javaVersion}"
         */
        private fun getDefaultVersion(): String {
            val baseMap = linkedMapOf(
                "Kusto.Java.Client" to getPackageVersion(),
                "Runtime.${escapeField(getRuntime())}" to getJavaVersion()
            )
            return formatHeader(baseMap)
        }

        /**
         * Gets the package version from the manifest or returns a default.
         */
        private fun getPackageVersion(): String {
            return try {
                ClientDetails::class.java.`package`.implementationVersion ?: "Unknown"
            } catch (e: Exception) {
                "Unknown"
            }
        }

        /**
         * Creates a ClientDetails from connector details
         * Example output: "Kusto.MyConnector:{1.0.0}|App.{MyApp}:{0.5.3}|CustomField:{CustomValue}"
         *
         * @param name The name of the connector (will be prefixed with "Kusto.")
         * @param version The version of the connector
         * @param sendUser True if the user should be sent to Kusto, otherwise "[none]" will be sent
         * @param overrideUser The user to send to Kusto, or null to use the current user
         * @param appName The app hosting the connector, or null to use the current process name
         * @param appVersion The version of the app hosting the connector, or null to use "[none]"
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
            additionalFields: Map<String, String>? = null
        ): ClientDetails {
            val fieldsMap = linkedMapOf<String, String>()
            fieldsMap["Kusto.$name"] = version

            val finalAppName = appName ?: getProcessName()
            val finalAppVersion = appVersion ?: NONE
            fieldsMap["App.${escapeField(finalAppName)}"] = finalAppVersion

            additionalFields?.let { fieldsMap.putAll(it) }

            val app = formatHeader(fieldsMap)

            val user = if (sendUser) {
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
                clientVersionForTracing = getDefaultVersion()
            )
        }
    }

    @JvmName("getApplicationForTracingOrDefault")
    fun getApplicationForTracing(): String {
        return applicationForTracing ?: getProcessName()
    }

    @JvmName("getUserNameForTracingOrDefault")
    fun getUserNameForTracing(): String {
        return userNameForTracing ?: getUserName()
    }

    @JvmName("getClientVersionForTracingOrDefault")
    fun getClientVersionForTracing(): String {
        val defaultVersion = getDefaultVersion()
        return if (clientVersionForTracing != null) {
            "$defaultVersion|$clientVersionForTracing"
        } else {
            defaultVersion
        }
    }
}
