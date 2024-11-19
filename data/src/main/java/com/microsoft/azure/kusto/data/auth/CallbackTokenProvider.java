// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;
import java.util.concurrent.Callable;

public class CallbackTokenProvider extends TokenProviderBase {
    public static final String CALLBACK_TOKEN_PROVIDER = "CallbackTokenProvider";
    private final @NotNull Callable<String> tokenProvider;

    CallbackTokenProvider(@NotNull String clusterUrl, @NotNull Callable<String> tokenProvider) throws URISyntaxException {
        super(clusterUrl, null);
        this.tokenProvider = tokenProvider;
    }

    @Override
    protected Mono<String> acquireAccessTokenImpl() {
        return Mono.fromCallable(tokenProvider).onErrorMap(e -> DataClientException.unwrapThrowable(clusterUrl, e));
    }

    @Override
    protected String getAuthMethod() {
        return CALLBACK_TOKEN_PROVIDER;
    }
}
