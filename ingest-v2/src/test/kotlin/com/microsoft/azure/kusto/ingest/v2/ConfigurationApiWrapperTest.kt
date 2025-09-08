/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.apis.DefaultApi
import com.microsoft.azure.kusto.ingest.v2.common.auth.TokenCredentialsProvider
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.ConfigurationResponse
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ConfigurationApiWrapperTest {
    private lateinit var defaultApi: DefaultApi
    private lateinit var wrapper: ConfigurationApiWrapper
    private val clusterUrl = "https://testcluster.kusto.windows.net"
    private val tokenProvider = mockk<TokenCredentialsProvider>(relaxed = true)

    @BeforeEach
    fun setup() {
        defaultApi = mockk(relaxed = true)
        wrapper = ConfigurationApiWrapper(clusterUrl, tokenProvider, false, defaultApi)
    }

    @Test
    fun `getConfigurationDetails returns configuration on success`() = runBlocking {
        val configResponse = ConfigurationResponse() // Fill with test data if needed
        val httpResponse = mockk<HttpResponse<ConfigurationResponse>>()
        every { httpResponse.success } returns true
        every { httpResponse.status } returns 200
        every { httpResponse.body() } returns configResponse
        coEvery { defaultApi.v1RestIngestionConfigurationGet() } returns httpResponse

        val result = wrapper.getConfigurationDetails()
        assertEquals(configResponse, result)
    }

    @Test
    fun `getConfigurationDetails throws IngestException on failure`() = runBlocking {
        val httpResponse = mockk<HttpResponse<ConfigurationResponse>>()
        every { httpResponse.success } returns false
        every { httpResponse.status } returns 500
        every { httpResponse.body() } returns null
        coEvery { defaultApi.v1RestIngestionConfigurationGet() } returns httpResponse

        val ex = assertThrows(IngestException::class.java) { runBlocking { wrapper.getConfigurationDetails() } }
        assertTrue(ex.message!!.contains("Failed to retrieve configuration details"))
    }
}
