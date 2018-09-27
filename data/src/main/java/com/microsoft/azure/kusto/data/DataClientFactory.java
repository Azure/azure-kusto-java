package com.microsoft.azure.kusto.data;

public class DataClientFactory {

    public static DataClient createClient(DataConnectionStringBuilder dcsb) {
        return new DataClientImpl(dcsb);
    }

}
