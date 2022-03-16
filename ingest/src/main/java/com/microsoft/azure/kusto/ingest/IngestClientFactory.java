// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;

import java.net.URISyntaxException;

public class IngestClientFactory {
    private IngestClientFactory() {
        // Hide the default constructor, as this is a factory with static methods
    }

    public static IngestClient createClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new QueuedIngestClient(csb);
    }

    public static StreamingIngestClient createStreamingIngestClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new StreamingIngestClient(csb);
    }

    public static ManagedStreamingIngestClient createManagedStreamingIngestClient(
            ConnectionStringBuilder dmConnectionStringBuilder,
            ConnectionStringBuilder engineConnectionStringBuilder) throws URISyntaxException {
        return new ManagedStreamingIngestClient(dmConnectionStringBuilder, engineConnectionStringBuilder);
    }
}
