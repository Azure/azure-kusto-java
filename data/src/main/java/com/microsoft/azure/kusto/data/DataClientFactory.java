package com.microsoft.azure.kusto.data;

public class DataClientFactory {

    public static DataClient createClient(DataConnectionStringBuilder kcsb) {
        return new DataClientImpl(kcsb);
    }

}
