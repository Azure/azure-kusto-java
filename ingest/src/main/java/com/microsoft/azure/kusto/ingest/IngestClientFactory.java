// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;

import java.net.URISyntaxException;

public class IngestClientFactory {

    public static IngestClient createClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new QueuedIngestClient(csb);
    }

    public static StreamingIngestClient createStreamingIngestClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new StreamingIngestClient(csb);
    }
}
