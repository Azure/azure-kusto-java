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
        return createClient(csb, null);
    }

    public static Client createClient(ConnectionStringBuilder csb, HttpClientProperties properties) throws URISyntaxException {
        return new ClientImpl(csb, properties);
    }

    public static StreamingClient createStreamingClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return createStreamingClient(csb, null);
    }

    public static StreamingClient createStreamingClient(ConnectionStringBuilder csb, HttpClientProperties properties) throws URISyntaxException {
        return new ClientImpl(csb, properties);
    }
}
