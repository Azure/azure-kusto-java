// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.HttpClientProperties;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;

public class IngestClientFactory {
    private IngestClientFactory() {
        // Hide the default constructor, as this is a factory with static methods
    }

    public static IngestClient createClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return createClient(csb, null);
    }

    public static IngestClient createClient(ConnectionStringBuilder csb, @Nullable HttpClientProperties properties) throws URISyntaxException {
        return new QueuedIngestClient(csb, properties);
    }

    public static StreamingIngestClient createStreamingIngestClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return createStreamingIngestClient(csb, null);
    }

    public static StreamingIngestClient createStreamingIngestClient(ConnectionStringBuilder csb, @Nullable HttpClientProperties properties) throws URISyntaxException {
        return new StreamingIngestClient(csb, properties);
    }

    public static ManagedStreamingIngestClient createManagedStreamingIngestClient(ConnectionStringBuilder dmConnectionStringBuilder,
            ConnectionStringBuilder engineConnectionStringBuilder)
        throws URISyntaxException {
        return createManagedStreamingIngestClient(dmConnectionStringBuilder, engineConnectionStringBuilder, null);
    }

    public static ManagedStreamingIngestClient createManagedStreamingIngestClient(ConnectionStringBuilder dmConnectionStringBuilder,
            ConnectionStringBuilder engineConnectionStringBuilder, @Nullable HttpClientProperties properties)
            throws URISyntaxException {
        return new ManagedStreamingIngestClient(dmConnectionStringBuilder, engineConnectionStringBuilder, properties);
    }

    public static ManagedStreamingIngestClient createManagedStreamingIngestClientFromEngineCsb(ConnectionStringBuilder engineConnectionStringBuilder)
            throws URISyntaxException {
        return createManagedStreamingIngestClientFromEngineCsb(engineConnectionStringBuilder, null);
    }

    public static ManagedStreamingIngestClient createManagedStreamingIngestClientFromEngineCsb(ConnectionStringBuilder engineConnectionStringBuilder,
            @Nullable HttpClientProperties properties)
            throws URISyntaxException {
        return ManagedStreamingIngestClient.fromEngineConnectionString(engineConnectionStringBuilder, properties);
    }

    public static ManagedStreamingIngestClient createManagedStreamingIngestClientFromDmCsb(ConnectionStringBuilder dmConnectionStringBuilder)
            throws URISyntaxException {
        return createManagedStreamingIngestClientFromDmCsb(dmConnectionStringBuilder, null);
    }

    public static ManagedStreamingIngestClient createManagedStreamingIngestClientFromDmCsb(ConnectionStringBuilder dmConnectionStringBuilder,
            @Nullable HttpClientProperties properties)
            throws URISyntaxException {
        return ManagedStreamingIngestClient.fromDmConnectionString(dmConnectionStringBuilder, properties);
    }
}
