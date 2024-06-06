package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.HttpClient;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;
import java.util.function.Function;

// TODO - Add to KCSB
public class AsyncCallbackTokenProvider extends TokenProviderBase {
    public static final String CALLBACK_TOKEN_PROVIDER = "CallbackTokenProvider";
    private final Function<HttpClient, Mono<String>> tokenProvider;

    AsyncCallbackTokenProvider(@NotNull String clusterUrl, @NotNull Function<HttpClient, Mono<String>> tokenProvider) throws URISyntaxException {
        super(clusterUrl, null);
        this.tokenProvider = tokenProvider;
    }

    @Override
    protected Mono<String> acquireAccessTokenImpl() {
        return tokenProvider.apply(httpClient)
                .onErrorMap(e -> new DataClientException(clusterUrl, e.getMessage(), e instanceof Exception ? (Exception) e : null));
    }

    @Override
    protected String getAuthMethod() {
        return CALLBACK_TOKEN_PROVIDER;
    }
}
