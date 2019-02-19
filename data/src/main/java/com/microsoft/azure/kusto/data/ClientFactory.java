package com.microsoft.azure.kusto.data;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

public class ClientFactory {

    public static Client createClient(ConnectionStringBuilder csb) throws URISyntaxException, MalformedURLException {
        return new ClientImpl(csb);
    }
}
