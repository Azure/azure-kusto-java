package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;

public class IngestClientFactory {

    public static IngestClient createClient(KustoConnectionStringBuilder kcsb) throws Exception {
        return new IngestClientImpl(kcsb);
    }
}
