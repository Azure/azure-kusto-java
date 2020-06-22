// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import java.net.URISyntaxException;

public class ClientFactory {

    public static Client createClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new ClientImpl(csb);
    }

    public static StreamingClient createStreamingClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new ClientImpl(csb);
    }
}
