package com.microsoft.azure.kusto.ingest.exceptions;

import java.util.List;

public class KustoClientAggregateException extends Exception{

    List<KustoClientException> kustoClientExceptions;

    public List<KustoClientException> getExceptions() { return kustoClientExceptions; }

    public KustoClientAggregateException(List<KustoClientException> kustoClientExceptions)
    {
        this.kustoClientExceptions = kustoClientExceptions;
    }
}
