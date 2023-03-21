package com.microsoft.azure.kusto.ingest;

import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.microsoft.azure.kusto.data.instrumentation.KustoTracer;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.*;

import java.util.HashMap;
import java.util.Map;

public abstract class IngestClientBase implements IngestClient{
    static final String INGEST_PREFIX = "ingest-";
    static final String PROTOCOL_SUFFIX = "://";


    static boolean shouldCompress(CompressionType sourceCompressionType, IngestionProperties.DataFormat dataFormat) {
        return (sourceCompressionType == null) && (dataFormat == null || dataFormat.isCompressible());
    }

    static String getIngestionEndpoint(String clusterUrl) {
        if (clusterUrl.contains(INGEST_PREFIX)) {
            return clusterUrl;
        } else {
            return clusterUrl.replaceFirst(PROTOCOL_SUFFIX, PROTOCOL_SUFFIX + INGEST_PREFIX);
        }
    }

    static String getQueryEndpoint(String clusterUrl) {
        if (clusterUrl.contains(INGEST_PREFIX)) {
            return clusterUrl.replaceFirst(INGEST_PREFIX, "");
        } else {
            return clusterUrl;
        }
    }
    protected abstract IngestionResult ingestFromFileImpl(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException; //should this be default because it is new

    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException{

        // trace ingestFromFile
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ingestFromFile", "complete");
        Context span = KustoTracer.startSpan("IngestClientBase.ingestFromFile", Context.NONE, ProcessKind.PROCESS, attributes);
        try {
            return ingestFromFileImpl(fileSourceInfo, ingestionProperties);
        } finally {
            KustoTracer.endSpan(null, span, null);
        }
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
    protected abstract IngestionResult ingestFromBlobImpl(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException{

        // trace ingestFromBlob
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ingestFromBlob", "complete");
        Context span = KustoTracer.startSpan("IngestClientBase.ingestFromBlob", Context.NONE, ProcessKind.PROCESS, attributes);
        try{
            return ingestFromBlobImpl(blobSourceInfo, ingestionProperties);
        } finally {
            KustoTracer.endSpan(null, span, null);
        }
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
    protected abstract IngestionResult ingestFromResultSetImpl(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException{

        // trace ingestFromResultSet
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ingestFromResultSet", "complete");
        Context span = KustoTracer.startSpan("IngestClientBase.ingestFromResultSet", Context.NONE, ProcessKind.PROCESS, attributes);
        try {
            return ingestFromResultSetImpl(resultSetSourceInfo, ingestionProperties);
        } finally {
            KustoTracer.endSpan(null, span, null);
        }
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
    protected abstract IngestionResult ingestFromStreamImpl(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException{

        // trace ingestFromStream
        Map<String, String> attributes = new HashMap<>();
        attributes.put("ingestFromStream", "complete");
        Context span = KustoTracer.startSpan("IngestClientBase.", Context.NONE, ProcessKind.PROCESS, attributes);
        try {
            return ingestFromStreamImpl(streamSourceInfo, ingestionProperties);
        } finally {
            KustoTracer.endSpan(null, span, null);
        }
    }
}
