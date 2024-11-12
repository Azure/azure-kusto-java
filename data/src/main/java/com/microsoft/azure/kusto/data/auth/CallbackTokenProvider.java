// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.HttpClient;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;

import com.microsoft.azure.kusto.data.exceptions.ExceptionsUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;
import java.util.concurrent.Callable;

public class CallbackTokenProvider extends TokenProviderBase {
    public static final String CALLBACK_TOKEN_PROVIDER = "CallbackTokenProvider";
    private final CallbackTokenProviderFunction tokenProvider;

    CallbackTokenProvider(@NotNull String clusterUrl, @NotNull Callable<String> tokenProvider) throws URISyntaxException {
        super(clusterUrl, null);
        this.tokenProvider = (httpClient) -> tokenProvider.call();
    }

    CallbackTokenProvider(@NotNull String clusterUrl, @NotNull CallbackTokenProviderFunction tokenProvider,
            @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
        this.tokenProvider = tokenProvider;
    }

    @Override
    protected Mono<String> acquireAccessTokenImpl() {
        return Mono.fromCallable(() -> tokenProvider.apply(httpClient))
                // TODO - is this a better way?
                .onErrorMap(e -> new DataClientException(clusterUrl, ExceptionsUtils.getMessageEx((Exception) e), (Exception) e));
    }

    @Override
    protected String getAuthMethod() {
        return CALLBACK_TOKEN_PROVIDER;
    }
}
