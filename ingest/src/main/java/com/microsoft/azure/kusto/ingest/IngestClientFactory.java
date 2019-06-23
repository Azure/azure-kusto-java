package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

public class IngestClientFactory {

    public static IngestClient createClient(ConnectionStringBuilder csb) throws URISyntaxException, MalformedURLException {
        return new IngestClientImpl(csb);
    }

    public static StreamingIngestClient createStreamingIngestClient(ConnectionStringBuilder csb) throws URISyntaxException, MalformedURLException {
        return new StreamingIngestClient(csb);
    }
}
