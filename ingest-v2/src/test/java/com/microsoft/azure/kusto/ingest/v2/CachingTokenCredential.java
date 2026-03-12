// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.AzureCliCredentialBuilder;
import reactor.core.publisher.Mono;

/**
 * Test-only credential: calls {@code az} once at class load, reuses the token forever.
 */
public class CachingTokenCredential implements TokenCredential {

    private static final AccessToken TOKEN;

    static {
        // Runs once on main thread before any reactor threads — .block() is safe here.
        TOKEN = new AzureCliCredentialBuilder().build()
                .getToken(new TokenRequestContext().addScopes("https://kusto.kusto.windows.net/.default"))
                .block();
    }

    public static final TokenCredential INSTANCE = new CachingTokenCredential();

    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        return Mono.just(TOKEN);
    }
}