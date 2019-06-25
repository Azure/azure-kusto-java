package com.microsoft.azure.kusto.data;

import java.net.URISyntaxException;

public class ClientFactory {

    public static Client createClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new ClientImpl(csb);
    }

    public static StreamingIngestProvider createStreamingIngestProvider(ConnectionStringBuilder csb) throws URISyntaxException {
        return new ClientImpl(csb);
    }
}
