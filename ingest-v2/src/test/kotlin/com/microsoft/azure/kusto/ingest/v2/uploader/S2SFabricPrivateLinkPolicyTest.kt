// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.azure.core.http.HttpClient
import com.azure.core.http.HttpMethod
import com.azure.core.http.HttpPipeline
import com.azure.core.http.HttpPipelineBuilder
import com.azure.core.http.HttpRequest
import com.azure.core.http.HttpResponse
import com.azure.core.util.Context
import com.microsoft.azure.kusto.ingest.v2.HEADER_MS_FABRIC_S2S_ACCESS_CONTEXT
import com.microsoft.azure.kusto.ingest.v2.HEADER_MS_S2S_ACTOR_AUTHORIZATION
import com.microsoft.azure.kusto.ingest.v2.common.models.S2SToken
import io.mockk.every
import io.mockk.mockk
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class S2SFabricPrivateLinkPolicyTest {

    /**
     * Fake HttpClient that captures every request it sends so tests can assert
     * which headers were present on the wire. Supports both the async and sync
     * pipeline paths.
     */
    private class CapturingHttpClient : HttpClient {
        val requests = ConcurrentLinkedQueue<HttpRequest>()

        private fun response(): HttpResponse = mockk(relaxed = true)

        override fun send(request: HttpRequest): Mono<HttpResponse> {
            requests.add(request)
            return Mono.just(response())
        }

        override fun sendSync(
            request: HttpRequest,
            context: Context,
        ): HttpResponse {
            requests.add(request)
            return response()
        }
    }

    private fun pipeline(
        client: HttpClient,
        provider: suspend () -> S2SToken,
        accessContext: String?,
    ): HttpPipeline =
        HttpPipelineBuilder()
            .httpClient(client)
            .policies(S2SFabricPrivateLinkPolicy(provider, accessContext))
            .build()

    private fun request(): HttpRequest =
        HttpRequest(
            HttpMethod.PUT,
            "https://onelake.dfs.fabric.microsoft.com/ws/lh/Files/blob",
        )

    @Test
    fun `processSync injects both S2S headers`() {
        val client = CapturingHttpClient()
        val pipeline =
            pipeline(
                client,
                { S2SToken.bearer("tok") },
                "ctx",
            )

        pipeline.sendSync(request(), Context.NONE)

        val sent = client.requests.single()
        assertEquals(
            "Bearer tok",
            sent.headers.getValue(HEADER_MS_S2S_ACTOR_AUTHORIZATION),
        )
        assertEquals(
            "ctx",
            sent.headers.getValue(HEADER_MS_FABRIC_S2S_ACCESS_CONTEXT),
        )
    }

    @Test
    fun `process injects both S2S headers`() {
        val client = CapturingHttpClient()
        val pipeline =
            pipeline(
                client,
                { S2SToken.bearer("tok") },
                "ctx",
            )

        pipeline.send(request()).block()

        val sent = client.requests.single()
        assertEquals(
            "Bearer tok",
            sent.headers.getValue(HEADER_MS_S2S_ACTOR_AUTHORIZATION),
        )
        assertEquals(
            "ctx",
            sent.headers.getValue(HEADER_MS_FABRIC_S2S_ACCESS_CONTEXT),
        )
    }

    @Test
    fun `set replaces a stale S2S authorization header`() {
        val client = CapturingHttpClient()
        val pipeline =
            pipeline(
                client,
                { S2SToken.bearer("fresh") },
                "ctx",
            )

        val req =
            request().apply {
                headers.set(HEADER_MS_S2S_ACTOR_AUTHORIZATION, "Bearer stale")
            }
        pipeline.sendSync(req, Context.NONE)

        assertEquals(
            "Bearer fresh",
            client.requests
                .single()
                .headers
                .getValue(HEADER_MS_S2S_ACTOR_AUTHORIZATION),
        )
    }

    @Test
    fun `null access context omits access context header`() {
        val client = CapturingHttpClient()
        val pipeline =
            pipeline(
                client,
                { S2SToken.bearer("tok") },
                null,
            )

        pipeline.sendSync(request(), Context.NONE)

        val sent = client.requests.single()
        assertEquals(
            "Bearer tok",
            sent.headers.getValue(HEADER_MS_S2S_ACTOR_AUTHORIZATION),
        )
        assertNull(sent.headers.getValue(HEADER_MS_FABRIC_S2S_ACCESS_CONTEXT))
    }

    @Test
    fun `provider exception propagates on sync path`() {
        val client = CapturingHttpClient()
        val pipeline =
            pipeline(
                client,
                { throw IllegalStateException("boom") },
                "ctx",
            )

        assertFailsWith<IllegalStateException> {
            pipeline.sendSync(request(), Context.NONE)
        }
        assertTrue(client.requests.isEmpty())
    }

    @Test
    fun `provider exception propagates on async path`() {
        val client = CapturingHttpClient()
        val pipeline =
            pipeline(
                client,
                { throw IllegalStateException("boom") },
                "ctx",
            )

        assertFailsWith<IllegalStateException> {
            pipeline.send(request()).block()
        }
        assertTrue(client.requests.isEmpty())
    }

    @Test
    fun `policy is safe under concurrent requests`() {
        val client = CapturingHttpClient()
        val pipeline =
            pipeline(
                client,
                { S2SToken.bearer("tok") },
                "ctx",
            )

        val threads =
            (1..16).map {
                Thread { pipeline.sendSync(request(), Context.NONE) }
            }
        threads.forEach { it.start() }
        threads.forEach { it.join() }

        assertEquals(16, client.requests.size)
        assertTrue(
            client.requests.all {
                it.headers.getValue(HEADER_MS_S2S_ACTOR_AUTHORIZATION) ==
                    "Bearer tok" &&
                    it.headers.getValue(
                        HEADER_MS_FABRIC_S2S_ACCESS_CONTEXT,
                    ) == "ctx"
            },
        )
    }
}
