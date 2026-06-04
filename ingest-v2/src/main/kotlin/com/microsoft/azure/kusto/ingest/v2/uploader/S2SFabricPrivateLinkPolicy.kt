// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.azure.core.http.HttpPipelineCallContext
import com.azure.core.http.HttpPipelineNextPolicy
import com.azure.core.http.HttpPipelineNextSyncPolicy
import com.azure.core.http.HttpResponse
import com.azure.core.http.policy.HttpPipelinePolicy
import com.microsoft.azure.kusto.ingest.v2.HEADER_MS_FABRIC_S2S_ACCESS_CONTEXT
import com.microsoft.azure.kusto.ingest.v2.HEADER_MS_S2S_ACTOR_AUTHORIZATION
import com.microsoft.azure.kusto.ingest.v2.common.models.S2SToken
import kotlinx.coroutines.runBlocking
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

/**
 * Pipeline policy that injects S2S (Service-to-Service) Fabric Private Link
 * headers into outgoing OneLake (Data Lake) storage requests.
 *
 * This mirrors the header names and value format used by [com.microsoft.azure.kusto.ingest.v2.KustoBaseApiClient]
 * for the main ingest endpoint, so that uploads to OneLake under a Fabric
 * Private Link perimeter are authorized the same way:
 * - `x-ms-s2s-actor-authorization` -> `"{scheme} {token}"`
 * - `x-ms-fabric-s2s-access-context` -> the access context string
 *
 * The token is retrieved per request via [s2sTokenProvider] so that long-running
 * or retried uploads always carry a fresh token. The provided callback must be
 * thread-safe, as the policy may be invoked concurrently for parallel uploads.
 *
 * @property s2sTokenProvider Suspend callback returning the current S2S token.
 * @property s2sFabricPrivateLinkAccessContext Optional access context describing
 *   the scope of the Fabric Private Link perimeter (e.g., tenant or workspace).
 */
internal class S2SFabricPrivateLinkPolicy(
    private val s2sTokenProvider: suspend () -> S2SToken,
    private val s2sFabricPrivateLinkAccessContext: String?,
) : HttpPipelinePolicy {

    private fun applyHeaders(context: HttpPipelineCallContext, token: S2SToken) {
        val headers = context.httpRequest.headers
        headers.set(HEADER_MS_S2S_ACTOR_AUTHORIZATION, token.toHeaderValue())
        s2sFabricPrivateLinkAccessContext?.let {
            headers.set(HEADER_MS_FABRIC_S2S_ACCESS_CONTEXT, it)
        }
    }

    override fun process(
        context: HttpPipelineCallContext,
        next: HttpPipelineNextPolicy,
    ): Mono<HttpResponse> =
        Mono.fromCallable { runBlocking { s2sTokenProvider() } }
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap { token ->
                applyHeaders(context, token)
                next.process()
            }

    override fun processSync(
        context: HttpPipelineCallContext,
        next: HttpPipelineNextSyncPolicy,
    ): HttpResponse {
        val token = runBlocking { s2sTokenProvider() }
        applyHeaders(context, token)
        return next.processSync()
    }
}
