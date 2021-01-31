// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import java.net.URISyntaxException;

public class ClientFactory {
    private ClientFactory() {
        // Hide the default constructor, as this is a factory with static methods
    }

    public static Client createClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new ClientImpl(csb);
    }

    public static StreamingClient createStreamingClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new ClientImpl(csb);
    }
}