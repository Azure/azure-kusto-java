// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;

import java.net.URISyntaxException;

public class IngestClientFactory {
    private IngestClientFactory() {
        // Hide the default constructor, as this is a factory with static methods
    }

    public static IngestClient createClient(ConnectionStringBuilder csb) throws DataClientException, URISyntaxException {
        return new QueuedIngestClient(csb);
    }

    public static StreamingIngestClient createStreamingIngestClient(ConnectionStringBuilder csb) throws DataClientException, URISyntaxException {
        return new StreamingIngestClient(csb);
    }

    public static ManagedStreamingIngestClient createManagedStreamingIngestClient(ConnectionStringBuilder dmConnectionStringBuilder,
                                                                                  ConnectionStringBuilder engineConnectionStringBuilder) throws DataClientException, URISyntaxException {
        return new ManagedStreamingIngestClient(dmConnectionStringBuilder, engineConnectionStringBuilder);
    }
}