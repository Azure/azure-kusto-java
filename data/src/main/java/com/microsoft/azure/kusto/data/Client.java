package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

public interface Client {

    KustoResponseResults execute(String command) throws DataServiceException, DataClientException;

    KustoResponseResults execute(String database, String command) throws DataServiceException, DataClientException;

    KustoResponseResults execute(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;
}
