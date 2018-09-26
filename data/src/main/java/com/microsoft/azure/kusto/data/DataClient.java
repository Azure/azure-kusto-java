package com.microsoft.azure.kusto.data;

public interface DataClient {

    DataResults execute(String command) throws Exception;

    DataResults execute(String database, String command) throws Exception;

}
