package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;

import java.net.URISyntaxException;

public class IngestClientFactory {

    public static IngestClient createClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new IngestClientImpl(csb);
    }

    public static StreamingIngestClient createStreamingIngestClient(ConnectionStringBuilder csb) throws URISyntaxException {
        return new StreamingIngestClient(csb);
    }
}
