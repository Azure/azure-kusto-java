package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import java.io.InputStream;

public interface Client {

    Results execute(String command) throws DataServiceException, DataClientException;

    Results execute(String database, String command) throws DataServiceException, DataClientException;

    Results execute(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    Results executeStreamingIngest(String database, String table, InputStream stream, String streamFormat, ClientRequestProperties properties, String mappingName, boolean leaveOpen) throws DataServiceException, DataClientException;
}
