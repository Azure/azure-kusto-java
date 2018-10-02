package com.microsoft.azure.kusto.data;

public class ClientFactory {

    public static Client createClient(ConnectionStringBuilder csb) {
        return new ClientImpl(csb);
    }

}
