package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

public interface Client {

    KustoResponseResultSet execute(String command) throws DataServiceException, DataClientException;

    KustoResponseResultSet execute(String database, String command) throws DataServiceException, DataClientException;

    KustoResponseResultSet execute(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;
}
