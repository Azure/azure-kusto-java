// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ClientDetailsTest {

    @Test
    fun `createDefault creates ClientDetails with default values`() {
        val clientDetails = ClientDetails.createDefault()

        assertNotNull(clientDetails)
        assertNotNull(clientDetails.applicationForTracing)
        assertNotNull(clientDetails.userNameForTracing)
        assertNull(clientDetails.clientVersionForTracing)
    }

    @Test
    fun `effectiveApplicationForTracing returns provided value when not null`() {
        val clientDetails =
            ClientDetails(
                applicationForTracing = "TestApp",
                userNameForTracing = "TestUser",
                clientVersionForTracing = null,
            )

        assertEquals("TestApp", clientDetails.effectiveApplicationForTracing)
    }

    @Test
    fun `effectiveApplicationForTracing returns default when null`() {
        val clientDetails =
            ClientDetails(
                applicationForTracing = null,
                userNameForTracing = "TestUser",
                clientVersionForTracing = null,
            )

        val result = clientDetails.effectiveApplicationForTracing
        assertNotNull(result)
        assertFalse(result.isBlank())
    }

    @Test
    fun `effectiveUserNameForTracing returns provided value when not null`() {
        val clientDetails =
            ClientDetails(
                applicationForTracing = "TestApp",
                userNameForTracing = "TestUser",
                clientVersionForTracing = null,
            )

        assertEquals("TestUser", clientDetails.effectiveUserNameForTracing)
    }

    @Test
    fun `effectiveUserNameForTracing returns default when null`() {
        val clientDetails =
            ClientDetails(
                applicationForTracing = "TestApp",
                userNameForTracing = null,
                clientVersionForTracing = null,
            )

        val result = clientDetails.effectiveUserNameForTracing
        assertNotNull(result)
    }

    @Test
    fun `effectiveClientVersionForTracing returns default version when null`() {
        val clientDetails =
            ClientDetails(
                applicationForTracing = "TestApp",
                userNameForTracing = "TestUser",
                clientVersionForTracing = null,
            )

        val version = clientDetails.effectiveClientVersionForTracing
        assertNotNull(version)
        assertTrue(version.contains("Kusto.Java.Client.V2"))
        assertTrue(version.contains("Runtime"))
    }

    @Test
    fun `effectiveClientVersionForTracing appends custom version when provided`() {
        val clientDetails =
            ClientDetails(
                applicationForTracing = "TestApp",
                userNameForTracing = "TestUser",
                clientVersionForTracing = "CustomVersion:1.0.0",
            )

        val version = clientDetails.effectiveClientVersionForTracing
        assertNotNull(version)
        assertTrue(version.contains("Kusto.Java.Client.V2"))
        assertTrue(version.contains("CustomVersion:1.0.0"))
    }

    @Test
    fun `fromConnectorDetails creates ClientDetails with basic info`() {
        val clientDetails =
            ClientDetails.fromConnectorDetails(
                name = "MyConnector",
                version = "1.0.0",
            )

        assertNotNull(clientDetails)
        assertNotNull(clientDetails.applicationForTracing)
        assertTrue(
            clientDetails.applicationForTracing!!.contains(
                "Kusto.MyConnector",
            ),
        )
        assertTrue(clientDetails.applicationForTracing.contains("1.0.0"))
        assertEquals(ClientDetails.NONE, clientDetails.userNameForTracing)
    }

    @Test
    fun `fromConnectorDetails includes user when sendUser is true`() {
        val clientDetails =
            ClientDetails.fromConnectorDetails(
                name = "MyConnector",
                version = "1.0.0",
                sendUser = true,
            )

        assertNotNull(clientDetails.userNameForTracing)
        assertNotEquals(ClientDetails.NONE, clientDetails.userNameForTracing)
    }

    @Test
    fun `fromConnectorDetails uses override user when provided`() {
        val clientDetails =
            ClientDetails.fromConnectorDetails(
                name = "MyConnector",
                version = "1.0.0",
                sendUser = true,
                overrideUser = "CustomUser@example.com",
            )

        assertEquals("CustomUser@example.com", clientDetails.userNameForTracing)
    }

    @Test
    fun `fromConnectorDetails includes appName and appVersion`() {
        val clientDetails =
            ClientDetails.fromConnectorDetails(
                name = "MyConnector",
                version = "1.0.0",
                appName = "MyApp",
                appVersion = "2.0.0",
            )

        assertNotNull(clientDetails.applicationForTracing)
        assertTrue(clientDetails.applicationForTracing!!.contains("MyApp"))
        assertTrue(clientDetails.applicationForTracing.contains("2.0.0"))
    }

    @Test
    fun `fromConnectorDetails includes additional fields`() {
        val additionalFields = mapOf("JobId" to "job-123", "RunId" to "run-456")

        val clientDetails =
            ClientDetails.fromConnectorDetails(
                name = "MyConnector",
                version = "1.0.0",
                additionalFields = additionalFields,
            )

        assertNotNull(clientDetails.applicationForTracing)
        assertTrue(clientDetails.applicationForTracing!!.contains("JobId"))
        assertTrue(clientDetails.applicationForTracing.contains("job-123"))
        assertTrue(clientDetails.applicationForTracing.contains("RunId"))
        assertTrue(clientDetails.applicationForTracing.contains("run-456"))
    }

    @Test
    fun `fromConnectorDetails formats fields with pipe separator`() {
        val clientDetails =
            ClientDetails.fromConnectorDetails(
                name = "MyConnector",
                version = "1.0.0",
            )

        assertNotNull(clientDetails.applicationForTracing)
        assertTrue(clientDetails.applicationForTracing!!.contains("|"))
    }

    @Test
    fun `fromConnectorDetails wraps values in curly braces`() {
        val clientDetails =
            ClientDetails.fromConnectorDetails(
                name = "MyConnector",
                version = "1.0.0",
            )

        assertNotNull(clientDetails.applicationForTracing)
        assertTrue(clientDetails.applicationForTracing!!.contains("{"))
        assertTrue(clientDetails.applicationForTracing.contains("}"))
    }

    @Test
    fun `data class equality works correctly`() {
        val client1 = ClientDetails("app1", "user1", "v1")
        val client2 = ClientDetails("app1", "user1", "v1")
        val client3 = ClientDetails("app2", "user1", "v1")

        assertEquals(client1, client2)
        assertNotEquals(client1, client3)
    }

    @Test
    fun `data class hashCode works correctly`() {
        val client1 = ClientDetails("app1", "user1", "v1")
        val client2 = ClientDetails("app1", "user1", "v1")

        assertEquals(client1.hashCode(), client2.hashCode())
    }

    @Test
    fun `data class copy works correctly`() {
        val original = ClientDetails("app1", "user1", "v1")
        val copied = original.copy(applicationForTracing = "app2")

        assertEquals("app2", copied.applicationForTracing)
        assertEquals("user1", copied.userNameForTracing)
        assertEquals("v1", copied.clientVersionForTracing)
    }

    @Test
    fun `fromConnectorDetails handles empty additional fields`() {
        val clientDetails =
            ClientDetails.fromConnectorDetails(
                name = "MyConnector",
                version = "1.0.0",
                additionalFields = emptyMap(),
            )

        assertNotNull(clientDetails)
        assertNotNull(clientDetails.applicationForTracing)
    }

    @Test
    fun `fromConnectorDetails handles null appVersion uses NONE`() {
        val clientDetails =
            ClientDetails.fromConnectorDetails(
                name = "MyConnector",
                version = "1.0.0",
                appName = "MyApp",
                appVersion = null,
            )

        assertNotNull(clientDetails.applicationForTracing)
        assertTrue(
            clientDetails.applicationForTracing!!.contains(
                ClientDetails.NONE,
            ),
        )
    }

    @Test
    fun `NONE constant has correct value`() {
        assertEquals("[none]", ClientDetails.NONE)
    }

    @Test
    fun `DEFAULT_APP_NAME constant has correct value`() {
        assertEquals("Kusto.Java.Client.V2", ClientDetails.DEFAULT_APP_NAME)
    }
}
