package com.microsoft.azure.kusto.data.client;

import com.microsoft.azure.kusto.data.results.DataResults;

public interface DataClient {

    DataResults execute(String command) throws Exception;

    DataResults execute(String database, String command) throws Exception;

}
