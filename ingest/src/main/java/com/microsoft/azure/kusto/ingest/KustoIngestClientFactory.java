package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.KustoConnectionStringBuilder;

public class KustoIngestClientFactory {

    public static KustoIngestClient createClient(KustoConnectionStringBuilder kcsb) {
        return new KustoIngestClientImpl(kcsb);
    }
}
