// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import reactor.core.publisher.Mono;

/**
 * A client for interacting with Kusto.
 */
public interface Client {

    /**
     * Executes a query against the default database.
     *
     * @param query The query command to execute.
     * @return The result of the query as a {@link KustoOperationResult}.
     * @throws DataServiceException If there is an error from the service.
     * @throws DataClientException If there is an error on the client side.
     */
    KustoOperationResult executeQuery(String query) throws DataServiceException, DataClientException;

    /**
     * Executes a query against the specified database.
     *
     * @param database The name of the database.
     * @param query The query to execute.
     * @return A {@link KustoOperationResult} with the result of the query.
     * @throws DataServiceException If there is an error from the service.
     * @throws DataClientException If there is an error on the client side.
     */
    KustoOperationResult executeQuery(String database, String query) throws DataServiceException, DataClientException;

    /**
     * Executes a query against a specified database with additional request properties.
     *
     * @param database The name of the database.
     * @param query The query query to execute.
     * @param properties Additional request properties.
     * @return A {@link KustoOperationResult} with the result of the query.
     * @throws DataServiceException If there is an error from the service.
     * @throws DataClientException If there is an error on the client side.
     */
    KustoOperationResult executeQuery(String database, String query, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    /**
     * Executes a query against the default database.
     *
     * @param command The query to execute.
     * @return A {@link KustoOperationResult} emitting the result of the query.
     */
    Mono<KustoOperationResult> executeQueryAsync(String command);

    /**
     * Executes a query against a specified database.
     *
     * @param database The name of the database.
     * @param command The query command to execute.
     * @return A {@link KustoOperationResult} with the result of the query.
     */
    Mono<KustoOperationResult> executeQueryAsync(String database, String command);

    /**
     * Executes a query command asynchronously against a specified database with additional request properties.
     *
     * @param database The name of the database.
     * @param command The query command to execute.
     * @param properties Additional request properties.
     * @return A {@link Mono} emitting the result of the query as a {@link KustoOperationResult}.
     */
    Mono<KustoOperationResult> executeQueryAsync(String database, String command, ClientRequestProperties properties);

    /**
     * Executes a management command against the default database.
     *
     * @param command The management command to execute.
     * @return The result of the command as a {@link KustoOperationResult}.
     * @throws DataServiceException If there is an error from the service.
     * @throws DataClientException If there is an error on the client side.
     */
    KustoOperationResult executeMgmt(String command) throws DataServiceException, DataClientException;

    /**
     * Executes a management command against a specified database.
     *
     * @param database The name of the database.
     * @param command The management command to execute.
     * @return The result of the command as a {@link KustoOperationResult}.
     * @throws DataServiceException If there is an error from the service.
     * @throws DataClientException If there is an error on the client side.
     */
    KustoOperationResult executeMgmt(String database, String command) throws DataServiceException, DataClientException;

    /**
     * Executes a management command against a specified database with additional request properties.
     *
     * @param database The name of the database.
     * @param command The management command to execute.
     * @param properties Additional request properties.
     * @return The result of the command as a {@link KustoOperationResult}.
     * @throws DataServiceException If there is an error from the service.
     * @throws DataClientException If there is an error on the client side.
     */
    KustoOperationResult executeMgmt(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    /**
     * Executes a management command asynchronously against the default database.
     *
     * @param command The management command to execute.
     * @return A {@link Mono} emitting the result of the command as a {@link KustoOperationResult}.
     */
    Mono<KustoOperationResult> executeMgmtAsync(String command);

    /**
     * Executes a management command asynchronously against a specified database.
     *
     * @param database The name of the database.
     * @param command The management command to execute.
     * @return A {@link Mono} emitting the result of the command as a {@link KustoOperationResult}.
     */
    Mono<KustoOperationResult> executeMgmtAsync(String database, String command);

    /**
     * Executes a management command asynchronously against a specified database with additional request properties.
     *
     * @param database The name of the database.
     * @param command The management command to execute.
     * @param properties Additional request properties.
     * @return A {@link Mono} emitting the result of the command as a {@link KustoOperationResult}.
     */
    Mono<KustoOperationResult> executeMgmtAsync(String database, String command, ClientRequestProperties properties);

    /**
     * Executes a query command and returns the result as a JSON string.
     *
     * @param database The name of the database.
     * @param command The query command to execute.
     * @param properties Additional request properties.
     * @return The result of the query as a JSON string.
     * @throws DataServiceException If there is an error from the service.
     * @throws DataClientException If there is an error on the client side.
     */
    String executeToJsonResult(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException;

    /**
     * Executes a query command asynchronously and returns the result as a JSON string.
     *
     * @param database The name of the database.
     * @param command The query command to execute.
     * @param properties Additional request properties.
     * @return A {@link Mono} emitting the result of the query as a JSON string.
     */
    Mono<String> executeToJsonResultAsync(String database, String command, ClientRequestProperties properties);
}
