package com.microsoft.azure.kusto.data;

public interface Client {

    Results execute(String command) throws Exception;

    Results execute(String database, String command) throws Exception;

}
