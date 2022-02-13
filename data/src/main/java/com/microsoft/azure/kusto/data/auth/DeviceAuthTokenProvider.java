// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.*;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import java.net.URISyntaxException;
import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;

public class DeviceAuthTokenProvider extends PublicAppTokenProviderBase {
    public DeviceAuthTokenProvider(@NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(clusterUrl, authorityId);
    }

    @Override
    protected IAuthenticationResult acquireNewAccessToken() {
        Consumer<DeviceCode> deviceCodeConsumer = (DeviceCode deviceCode) -> {
            System.out.println(deviceCode.message());
        };

        DeviceCodeFlowParameters deviceCodeFlowParams =
                DeviceCodeFlowParameters.builder(scopes, deviceCodeConsumer).build();
        return clientApplication.acquireToken(deviceCodeFlowParams).join();
    }
}
