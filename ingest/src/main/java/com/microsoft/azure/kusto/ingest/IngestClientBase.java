package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.exceptions.ExceptionUtils;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import org.apache.http.conn.util.InetAddressUtils;

import java.io.IOException;
import java.net.URI;
import com.microsoft.azure.kusto.data.instrumentation.SupplierTwoExceptions;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.*;

import java.util.HashMap;
import java.util.Map;

public abstract class IngestClientBase implements IngestClient {
    static final String INGEST_PREFIX = "ingest-";
    static final String PROTOCOL_SUFFIX = "://";

    static boolean shouldCompress(CompressionType sourceCompressionType, IngestionProperties.DataFormat dataFormat) {
        return (sourceCompressionType == null) && (dataFormat == null || dataFormat.isCompressible());
    }

    static String getIngestionEndpoint(String clusterUrl) {
        if (clusterUrl == null || clusterUrl.contains(INGEST_PREFIX) || isReservedHostname(clusterUrl)) {
            return clusterUrl;
        }
        if (clusterUrl.contains(PROTOCOL_SUFFIX)) {
            return clusterUrl.replaceFirst(PROTOCOL_SUFFIX, PROTOCOL_SUFFIX + INGEST_PREFIX);
        }
        return INGEST_PREFIX + clusterUrl;
    }

    static String getQueryEndpoint(String clusterUrl) {
        return (clusterUrl == null || isReservedHostname(clusterUrl)) ? clusterUrl : clusterUrl.replaceFirst(INGEST_PREFIX, "");
    }

    static boolean isReservedHostname(String rawUri) {
        URI uri = URI.create(rawUri);
        if (!uri.isAbsolute()) {
            return true;
        }

        String authority = uri.getAuthority().toLowerCase();
        boolean isIPFlag;
        if (authority.startsWith("[") && authority.endsWith("]")) {
            authority = authority.substring(1, authority.length() - 1);
            isIPFlag = true;
        } else {
            isIPFlag = InetAddressUtils.isIPv4Address(authority);
        }

        boolean isLocalFlag = authority.contains("localhost");

        return isLocalFlag || isIPFlag || authority.equalsIgnoreCase("onebox.dev.kusto.windows.net");
    }

    protected abstract IngestionResult ingestFromFileImpl(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        // trace ingestFromFile
        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<IngestionResult, IngestionClientException, IngestionServiceException>) () -> ingestFromFileImpl(fileSourceInfo,
                        ingestionProperties),
                getClientType().concat(".ingestFromFile"));
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
            throws IngestionClientException, IngestionServiceException {
        // trace ingestFromBlob
        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<IngestionResult, IngestionClientException, IngestionServiceException>) () -> ingestFromBlobImpl(blobSourceInfo,
                        ingestionProperties),
                getClientType().concat(".ingestFromBlob"));
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
            throws IngestionClientException, IngestionServiceException {
        // trace ingestFromResultSet
        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<IngestionResult, IngestionClientException, IngestionServiceException>) () -> ingestFromResultSetImpl(resultSetSourceInfo,
                        ingestionProperties),
                getClientType().concat(".ingestFromResultSet"));
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
            throws IngestionClientException, IngestionServiceException, IOException;

    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        // trace ingestFromStream
        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<IngestionResult, IngestionClientException, IngestionServiceException>) () -> {
                    try {
                        return ingestFromStreamImpl(streamSourceInfo,
                                ingestionProperties);
                    } catch (IOException e) {
                        throw new IngestionServiceException(ExceptionUtils.getMessageEx(e), e);
                    }
                },
                getClientType().concat(".ingestFromStream"));
    }

    protected Map<String, String> getIngestionTraceAttributes(TraceableAttributes sourceInfo, TraceableAttributes ingestionProperties) {
        Map<String, String> attributes = new HashMap<>();
        if (sourceInfo != null) {
            attributes.putAll(sourceInfo.getTracingAttributes());
        }
        if (ingestionProperties != null) {
            attributes.putAll(ingestionProperties.getTracingAttributes());
        }
        return attributes;
    }

    protected abstract String getClientType();
}
