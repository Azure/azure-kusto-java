// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.req.KustoQuery;
import reactor.core.publisher.Mono;

import java.io.Closeable;

public interface Client extends Closeable {

    KustoOperationResult execute(KustoQuery kq) throws DataServiceException, DataClientException;

    KustoOperationResult executeQuery(KustoQuery kq) throws DataServiceException, DataClientException;

    KustoOperationResult executeMgmt(KustoQuery kq) throws DataServiceException, DataClientException;

    Mono<KustoOperationResult> executeAsync(KustoQuery kq);

    Mono<KustoOperationResult> executeQueryAsync(KustoQuery kq);

    Mono<KustoOperationResult> executeMgmtAsync(KustoQuery kq);

    String executeToJsonResult(KustoQuery kq) throws DataServiceException, DataClientException;

    Mono<String> executeToJsonResultAsync(KustoQuery kq);

    @Deprecated
    KustoOperationResult execute(String command) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult execute(String database, String command) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult execute(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult executeQuery(String command) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult executeQuery(String database, String command) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult executeQuery(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult executeMgmt(String command) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult executeMgmt(String database, String command) throws DataServiceException, DataClientException;

    @Deprecated
    KustoOperationResult executeMgmt(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    @Deprecated
    String executeToJsonResult(String database) throws DataServiceException, DataClientException;

    @Deprecated
    String executeToJsonResult(String database, String command) throws DataServiceException, DataClientException;

    @Deprecated
    String executeToJsonResult(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;
}
