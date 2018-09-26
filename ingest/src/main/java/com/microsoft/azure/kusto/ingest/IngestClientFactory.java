package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.DataConnectionStringBuilder;

public class IngestClientFactory {

    public static IngestClient createClient(DataConnectionStringBuilder kcsb) throws Exception {
        return new IngestClientImpl(kcsb);
    }
}
