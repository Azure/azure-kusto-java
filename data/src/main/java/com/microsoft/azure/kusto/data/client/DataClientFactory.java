package com.microsoft.azure.kusto.data.client;

import com.microsoft.azure.kusto.data.connection.DataConnectionStringBuilder;

public class DataClientFactory {

    public static DataClient createClient(DataConnectionStringBuilder dcsb) {
        return new DataClientImpl(dcsb);
    }

}
