// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.AzureCliCredentialBuilder;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * A test-only {@link TokenCredential} wrapper that caches tokens per scope to prevent concurrent
 * subprocess invocations when the underlying credential (e.g. {@code AzureCliCredential})
 * is called from multiple parallel test threads simultaneously.
 *
 * <p>The token is proactively refreshed {@value #TOKEN_REFRESH_MARGIN_MINUTES} minutes before
 * expiry. All concurrent callers for the same scope wait on the same {@link ReentrantLock} —
 * only one subprocess invocation occurs at a time (double-checked locking).
 *
 * <p>Token caching is intentionally placed here (test layer) rather than in
 * {@code KustoBaseApiClient}, keeping token lifecycle management as the responsibility of the
 * {@link TokenCredential} implementor.
 */
public class CachingTokenCredential implements TokenCredential {

    /** Process-wide shared instance wrapping {@code AzureCliCredential}, used by all E2E tests. */
    public static final TokenCredential INSTANCE =
            new CachingTokenCredential(new AzureCliCredentialBuilder().build());

    private static final long TOKEN_REFRESH_MARGIN_MINUTES = 5L;

    private final TokenCredential delegate;
    private final ReentrantLock lock = new ReentrantLock();
    // Per-scope cache: scope string → AccessToken
    private final Map<String, AccessToken> tokenCache = new ConcurrentHashMap<>();

    public CachingTokenCredential(TokenCredential delegate) {
        this.delegate = delegate;
    }

    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        String scopeKey = request.getScopes().stream().collect(Collectors.joining(","));

        // Fast path: return cached token without acquiring lock if not near expiry
        AccessToken current = tokenCache.get(scopeKey);
        if (current != null && current.getExpiresAt().isAfter(
                OffsetDateTime.now().plusMinutes(TOKEN_REFRESH_MARGIN_MINUTES))) {
            return Mono.just(current);
        }

        // Slow path: acquire lock, refresh if still needed (double-checked locking)
        return Mono.fromCallable(() -> {
            lock.lock();
            try {
                AccessToken recheck = tokenCache.get(scopeKey);
                if (recheck != null && recheck.getExpiresAt().isAfter(
                        OffsetDateTime.now().plusMinutes(TOKEN_REFRESH_MARGIN_MINUTES))) {
                    return recheck;
                }
                // Only one thread reaches here per scope at a time
                AccessToken newToken = delegate.getToken(request).block();
                if (newToken == null) {
                    throw new IllegalStateException("TokenCredential.getToken() returned null");
                }
                tokenCache.put(scopeKey, newToken);
                return newToken;
            } finally {
                lock.unlock();
            }
        });
    }
}