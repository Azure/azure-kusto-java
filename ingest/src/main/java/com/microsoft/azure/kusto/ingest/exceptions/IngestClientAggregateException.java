package com.microsoft.azure.kusto.ingest.exceptions;

import java.util.List;

public class IngestClientAggregateException extends Exception{

    List<IngestClientException> ingestClientExceptions;

    public List<IngestClientException> getExceptions() { return ingestClientExceptions; }

    public IngestClientAggregateException(List<IngestClientException> ingestClientExceptions)
    {
        this.ingestClientExceptions = ingestClientExceptions;
    }
}
