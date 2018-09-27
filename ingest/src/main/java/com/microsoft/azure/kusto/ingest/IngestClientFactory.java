package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;

public class IngestClientFactory {

    public static IngestClient createClient(ConnectionStringBuilder dcsb) throws Exception {
        return new IngestClientImpl(dcsb);
    }
}
