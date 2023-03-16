// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.microsoft.azure.kusto.data.instrumentation.KustoTracer;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public interface IngestClient extends Closeable {

    /**
     * <p>Ingest data from a file into Kusto database.</p>
     * This method ingests the data from a given file, described in {@code fileSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param fileSourceInfo      The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     * @see FileSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromFileImpl(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException; //should this be default because it is new

    default IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException{

        // trace ingestFromFile
        KustoTracer kustoTracer = KustoTracer.getInstance();
        Context span = kustoTracer.startSpan("ingestFromFile", Context.NONE, ProcessKind.PROCESS);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ingestFromFile", "complete");
        kustoTracer.setAttributes(attributes, span);

        IngestionResult ingestionResult;
        try {
            ingestionResult = ingestFromFileImpl(fileSourceInfo, ingestionProperties);
        } finally {
            kustoTracer.endSpan(null, span, null);
        }
        return ingestionResult;
    }

    /**
     * <p>Ingest data from a blob storage into Kusto database.</p>
     * This method ingests the data from a given blob, described in {@code blobSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param blobSourceInfo      The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     * @see BlobSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromBlobImpl(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

    default IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException{

        // trace ingestFromBlob
        KustoTracer kustoTracer = KustoTracer.getInstance();
        Context span = kustoTracer.startSpan("ingestFromBlob", Context.NONE, ProcessKind.PROCESS);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ingestFromBlob", "complete");
        kustoTracer.setAttributes(attributes, span);
        IngestionResult ingestionResult;
        try{
            ingestionResult = ingestFromBlobImpl(blobSourceInfo, ingestionProperties);
        } finally {
            kustoTracer.endSpan(null, span, null);
        }
        return ingestionResult;
    }

    /**
     * <p>Ingest data from a Result Set into Kusto database.</p>
     * This method ingests the data from a given Result Set, described in {@code resultSetSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     * <p>
     * Ingesting from ResultSet is equivalent to ingesting from a csv stream.
     * The DataFormat should be empty or set to "csv", and the mapping, should it be provided, should be csv mapping.
     *
     * @param resultSetSourceInfo The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     * @see ResultSetSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromResultSetImpl(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

    default IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException{

        // trace ingestFromResultSet
        KustoTracer kustoTracer = KustoTracer.getInstance();
        Context span = kustoTracer.startSpan("ingestFromResultSet", Context.NONE, ProcessKind.PROCESS);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ingestFromResultSet", "complete");
        kustoTracer.setAttributes(attributes, span);
        IngestionResult ingestionResult;
        try{
            ingestionResult = ingestFromResultSetImpl(resultSetSourceInfo, ingestionProperties);
        } finally {
            kustoTracer.endSpan(null, span, null);
        }
        return ingestionResult;
    }

    /**
     * <p>Ingest data from an input stream, into Kusto database.</p>
     * This method ingests the data from a given input stream, described in {@code streamSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param streamSourceInfo    The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     * @see StreamSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromStreamImpl(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

    default IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException{

        // trace ingestFromStream
        KustoTracer kustoTracer = KustoTracer.getInstance();
        Context span = kustoTracer.startSpan("ingestFromStream", Context.NONE, ProcessKind.PROCESS);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ingestFromStream", "complete");
        kustoTracer.setAttributes(attributes, span);
        IngestionResult ingestionResult;
        try{
            ingestionResult = ingestFromStreamImpl(streamSourceInfo, ingestionProperties);
        } finally {
            kustoTracer.endSpan(null, span, null);
        }
        return ingestionResult;
    }
}
