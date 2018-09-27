package com.microsoft.azure.kusto.data;

public class ClientFactory {

    public static Client createClient(ConnectionStringBuilder dcsb) {
        return new ClientImpl(dcsb);
    }

}
