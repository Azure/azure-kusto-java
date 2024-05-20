// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.req.KustoQuery;
import reactor.core.publisher.Mono;

public interface Client {

    Mono<KustoOperationResult> executeQueryAsync(KustoQuery kq);

    Mono<KustoOperationResult> executeMgmtAsync(KustoQuery kq);

    Mono<String> executeToJsonAsync(KustoQuery kq);

    @Deprecated
    KustoOperationResult executeQuery(String command) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult executeQuery(String database, String command) throws DataServiceException, DataClientException;

    KustoOperationResult executeQuery(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult executeMgmt(String command) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult executeMgmt(String database, String command) throws DataServiceException, DataClientException;

    KustoOperationResult executeMgmt(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    @Deprecated
    String executeToJsonResult(String database) throws DataServiceException, DataClientException;

    @Deprecated
    String executeToJsonResult(String database, String command) throws DataServiceException, DataClientException;

    String executeToJsonResult(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;
}
