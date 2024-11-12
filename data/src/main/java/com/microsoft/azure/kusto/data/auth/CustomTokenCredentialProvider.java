package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;

public class CustomTokenCredentialProvider extends TokenProviderBase {
    private final TokenCredential credential;
    private final TokenRequestContext context;

    CustomTokenCredentialProvider(@NotNull String clusterUrl, @NotNull TokenCredential credential, @Nullable TokenRequestContext context) throws URISyntaxException {
        super(clusterUrl, null);
        this.credential = credential;
        this.context = context;
    }

    @Override
    protected Mono<String> acquireAccessTokenImpl() {
        return credential.getToken(context).map(AccessToken::getToken);
    }
}
