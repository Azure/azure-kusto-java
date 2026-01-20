// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.auth.endpoints

import kotlinx.serialization.SerialName
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlinx.serialization.Serializable as KSerializable

/** Data class representing the structure of WellKnownKustoEndpoints.json */
@KSerializable
data class AllowedEndpoints(
    @SerialName("AllowedKustoSuffixes")
    val allowedKustoSuffixes: List<String> = emptyList(),
    @SerialName("AllowedKustoHostnames")
    val allowedKustoHostnames: List<String> = emptyList(),
)

@KSerializable
data class WellKnownKustoEndpointsData(
    @SerialName("_Comments") val comments: List<String> = emptyList(),
    @SerialName("AllowedEndpointsByLogin")
    val allowedEndpointsByLogin: Map<String, AllowedEndpoints> = emptyMap(),
) {
    companion object {
        private val logger =
            LoggerFactory.getLogger(WellKnownKustoEndpointsData::class.java)

        @Volatile private var instance: WellKnownKustoEndpointsData? = null

        private val json = Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

        fun getInstance(): WellKnownKustoEndpointsData {
            return instance
                ?: synchronized(this) {
                    instance ?: readInstance().also { instance = it }
                }
        }

        private fun readInstance(): WellKnownKustoEndpointsData {
            return try {
                val resourceStream =
                    WellKnownKustoEndpointsData::class
                        .java
                        .getResourceAsStream(
                            "/WellKnownKustoEndpoints.json",
                        )
                        ?: throw RuntimeException(
                            "WellKnownKustoEndpoints.json not found in classpath",
                        )

                val content =
                    resourceStream.bufferedReader().use { it.readText() }
                json.decodeFromString<WellKnownKustoEndpointsData>(content)
            } catch (ex: Exception) {
                logger.error("Failed to read WellKnownKustoEndpoints.json", ex)
                throw RuntimeException(
                    "Failed to read WellKnownKustoEndpoints.json",
                    ex,
                )
            }
        }
    }
}
