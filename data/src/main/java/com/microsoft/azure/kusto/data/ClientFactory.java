// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;

import java.net.URISyntaxException;

public class ClientFactory {
    private ClientFactory() {
        // Disallow instantiating class
    }

    public static Client createClient(ConnectionStringBuilder csb) throws DataClientException, URISyntaxException {
        return new ClientImpl(csb);
    }

    public static StreamingClient createStreamingClient(ConnectionStringBuilder csb) throws DataClientException, URISyntaxException {
        return new ClientImpl(csb);
    }
}