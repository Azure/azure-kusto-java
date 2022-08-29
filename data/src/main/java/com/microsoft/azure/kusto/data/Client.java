// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import java.io.Closeable;

public interface Client extends Closeable {
    KustoOperationResult execute(String command) throws DataServiceException, DataClientException;

    KustoOperationResult execute(String database, String command) throws DataServiceException, DataClientException;

    KustoOperationResult execute(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    String executeToJsonResult(String database) throws DataServiceException, DataClientException;

    String executeToJsonResult(String database, String command) throws DataServiceException, DataClientException;

    String executeToJsonResult(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;
}
