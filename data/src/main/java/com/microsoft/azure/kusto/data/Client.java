// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import reactor.core.publisher.Mono;

public interface Client {

    KustoOperationResult executeQuery(String command);

    KustoOperationResult executeQuery(String database, String command);

    KustoOperationResult executeQuery(String database, String command, ClientRequestProperties properties);

    Mono<KustoOperationResult> executeQueryAsync(String command);

    Mono<KustoOperationResult> executeQueryAsync(String database, String command);

    Mono<KustoOperationResult> executeQueryAsync(String database, String command, ClientRequestProperties properties);

    KustoOperationResult executeMgmt(String command);

    KustoOperationResult executeMgmt(String database, String command);

    KustoOperationResult executeMgmt(String database, String command, ClientRequestProperties properties);

    Mono<KustoOperationResult> executeMgmtAsync(String command);

    Mono<KustoOperationResult> executeMgmtAsync(String database, String command);

    Mono<KustoOperationResult> executeMgmtAsync(String database, String command, ClientRequestProperties properties);

    String executeToJsonResult(String database, String command, ClientRequestProperties properties);

    Mono<String> executeToJsonResultAsync(String database, String command, ClientRequestProperties properties);
}
