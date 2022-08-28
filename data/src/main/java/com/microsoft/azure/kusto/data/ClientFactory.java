// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

import java.net.URISyntaxException;

public class ClientFactory {
    private ClientFactory() {
        // Hide the default constructor, as this is a factory with static methods
    }

    /**
     * Creates a new {@linkplain Client} instance with the given connection string. The underlying HTTP client is
     * created with default settings.
     *
     * @param csb the connection string builder
     * @return a fully constructed {@linkplain Client} instance
     * @throws URISyntaxException if the cluster URL is invalid
     */
    public static Client createClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return createClient(csb, (HttpClientProperties) null);
    }

    /**
     * Creates a new {@linkplain Client} instance with the given connection string. The underlying HTTP client is
     * customized with the given properties.
     *
     * @param csb the connection string builder
     * @param properties HTTP client properties
     * @return a fully constructed {@linkplain Client} instance
     * @throws URISyntaxException if the cluster URL is invalid
     */
    public static Client createClient(ConnectionStringBuilder csb, HttpClientProperties properties) throws URISyntaxException {
        return new ClientImpl(csb, properties);
    }

    /**
     * Creates a new {@linkplain Client} instance with the given connection string. The underlying HTTP client is
     * customized with the given properties.
     *
     * @param csb the connection string builder
     * @param client CloseableHttpClient client. It will not be closed when {@link Client#close} is called.
     * @return a fully constructed {@linkplain Client} instance
     * @throws URISyntaxException if the cluster URL is invalid
     */
    public static Client createClient(ConnectionStringBuilder csb, CloseableHttpClient client) throws URISyntaxException {
        return new ClientImpl(csb, client, true);
    }

    /**
     * Creates a new {@linkplain StreamingClient} instance with the given connection string. The underlying HTTP client
     * is created with default settings.
     *
     * @param csb the connection string builder
     * @return a fully constructed {@linkplain StreamingClient} instance
     * @throws URISyntaxException if the cluster URL is invalid
     */
    public static StreamingClient createStreamingClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return createStreamingClient(csb, null);
    }

    /**
     * Creates a new {@linkplain StreamingClient} instance with the given connection string. The underlying HTTP client
     * is customized with the given properties.
     *
     * @param csb the connection string builder
     * @param properties HTTP client properties
     * @return a fully constructed {@linkplain StreamingClient} instance
     * @throws URISyntaxException if the cluster URL is invalid
     */
    public static StreamingClient createStreamingClient(ConnectionStringBuilder csb, HttpClientProperties properties) throws URISyntaxException {
        return new ClientImpl(csb, properties);
    }
}
