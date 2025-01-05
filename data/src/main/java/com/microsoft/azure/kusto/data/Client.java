// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import reactor.core.publisher.Mono;

public interface Client {

    KustoOperationResult executeQuery(String command) throws DataServiceException, DataClientException;

    KustoOperationResult executeQuery(String database, String command) throws DataServiceException, DataClientException;

    KustoOperationResult executeQuery(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    Mono<KustoOperationResult> executeQueryAsync(String command) throws DataServiceException, DataClientException;

    Mono<KustoOperationResult> executeQueryAsync(String database, String command) throws DataServiceException, DataClientException;

    Mono<KustoOperationResult> executeQueryAsync(String database, String command, ClientRequestProperties properties)
            throws DataServiceException, DataClientException;

    KustoOperationResult executeMgmt(String command) throws DataServiceException, DataClientException;

    KustoOperationResult executeMgmt(String database, String command) throws DataServiceException, DataClientException;

    KustoOperationResult executeMgmt(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    Mono<KustoOperationResult> executeMgmtAsync(String command) throws DataServiceException, DataClientException;

    Mono<KustoOperationResult> executeMgmtAsync(String database, String command) throws DataServiceException, DataClientException;

    Mono<KustoOperationResult> executeMgmtAsync(String database, String command, ClientRequestProperties properties)
            throws DataServiceException, DataClientException;

    String executeToJsonResult(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    Mono<String> executeToJsonResultAsync(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;
}
