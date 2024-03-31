package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.HttpClient;
import com.microsoft.aad.msal4j.*;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.microsoft.aad.msal4j.IAuthenticationResult;

import java.net.URISyntaxException;
import java.util.function.Consumer;

public class DeviceAuthTokenProvider extends PublicAppTokenProviderBase {

    public static final String DEVICE_AUTH_TOKEN_PROVIDER = "DeviceAuthTokenProvider";

    public DeviceAuthTokenProvider(@NotNull String clusterUrl, String authorityId, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, authorityId, httpClient);
    }

    @Override
    protected IAuthenticationResult acquireNewAccessToken() {
        Consumer<DeviceCode> deviceCodeConsumer = (DeviceCode deviceCode) -> {
            System.out.println(deviceCode.message());
        };

        DeviceCodeFlowParameters deviceCodeFlowParams = DeviceCodeFlowParameters.builder(scopes, deviceCodeConsumer).build();
        return clientApplication.acquireToken(deviceCodeFlowParams).join();
    }

    @Override
    protected String getAuthMethod() {
        return DEVICE_AUTH_TOKEN_PROVIDER;
    }
}
