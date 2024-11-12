package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.ExceptionsUtils;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;

public class AsyncCallbackTokenProvider extends TokenProviderBase {
    public static final String CALLBACK_TOKEN_PROVIDER = "CallbackTokenProvider";
    private final Mono<String> tokenProvider;

    AsyncCallbackTokenProvider(@NotNull String clusterUrl, @NotNull Mono<String> tokenProvider) throws URISyntaxException {
        super(clusterUrl, null);
        this.tokenProvider = tokenProvider;
    }

    @Override
    protected Mono<String> acquireAccessTokenImpl() {
        return tokenProvider
                .onErrorMap(e -> {
                    if (e instanceof Exception) {
                        Exception ex = (Exception) e;
                        return new DataClientException(clusterUrl, ExceptionsUtils.getMessageEx(ex), ex);
                    } else {
                        return new DataClientException(clusterUrl, e.toString(), null);
                    }
                });
    }

    @Override
    protected String getAuthMethod() {
        return CALLBACK_TOKEN_PROVIDER;
    }
}
