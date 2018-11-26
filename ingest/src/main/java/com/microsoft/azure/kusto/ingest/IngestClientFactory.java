package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;

public class IngestClientFactory {

    public static IngestClient createClient(ConnectionStringBuilder csb) {
        return new IngestClientImpl(csb);
    }
}
