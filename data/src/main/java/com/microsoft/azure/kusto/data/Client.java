package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

public interface Client {

    Results execute(String command) throws DataServiceException, DataClientException;

    Results execute(String database, String command) throws DataServiceException, DataClientException;

}
