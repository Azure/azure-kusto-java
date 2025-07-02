package com.microsoft.azure.kusto.ingest;

import com.azure.core.util.CoreUtils;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import org.apache.http.conn.util.InetAddressUtils;

import java.net.URI;

import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import reactor.core.publisher.Mono;

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
        boolean isIpAddress;
        if (authority.startsWith("[") && authority.endsWith("]")) {
            isIpAddress = true;
        } else {
            isIpAddress = InetAddressUtils.isIPv4Address(authority);
        }

        boolean isLocalhost = authority.contains("localhost");
        String host = CoreUtils.isNullOrEmpty(uri.getHost()) ? "" : uri.getHost();

        return isLocalhost || isIpAddress || host.equalsIgnoreCase("onebox.dev.kusto.windows.net");
    }

    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) {
        return ingestFromFileAsync(fileSourceInfo, ingestionProperties).block();
    }

    protected abstract Mono<IngestionResult> ingestFromFileAsyncImpl(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties);

    public Mono<IngestionResult> ingestFromFileAsync(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) {
        // trace ingestFromFileAsync
        return Mono.defer(() -> MonitoredActivity.wrap(
                ingestFromFileAsyncImpl(fileSourceInfo,
                        ingestionProperties),
                getClientType().concat(".ingestFromFile")));
    }

    /**
     * <p>Ingest data from a blob storage into Kusto database.</p>
     * This method ingests the data from a given blob, described in {@code blobSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param blobSourceInfo      The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @see BlobSourceInfo
     * @see IngestionProperties
     */
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) {
        return ingestFromBlobAsync(blobSourceInfo, ingestionProperties).block();
    }

    protected abstract Mono<IngestionResult> ingestFromBlobAsyncImpl(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties);

    public Mono<IngestionResult> ingestFromBlobAsync(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) {
        // trace ingestFromBlob
        return Mono.defer(() -> MonitoredActivity.wrap(
                ingestFromBlobAsyncImpl(blobSourceInfo,
                        ingestionProperties),
                getClientType().concat(".ingestFromBlob")));
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
     * @see ResultSetSourceInfo
     * @see IngestionProperties
     */
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) {
        return ingestFromResultSetAsync(resultSetSourceInfo, ingestionProperties).block();
    }

    protected abstract Mono<IngestionResult> ingestFromResultSetAsyncImpl(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties);

    public Mono<IngestionResult> ingestFromResultSetAsync(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) {
        // trace ingestFromResultSet
        return Mono.defer(() -> MonitoredActivity.wrap(
                ingestFromResultSetAsyncImpl(resultSetSourceInfo,
                        ingestionProperties),
                getClientType().concat(".ingestFromResultSet")));
    }

    /**
     * <p>Ingest data from an input stream, into Kusto database.</p>
     * This method ingests the data from a given input stream, described in {@code streamSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param streamSourceInfo    The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @see StreamSourceInfo
     * @see IngestionProperties
     */
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) {
        return ingestFromStreamAsync(streamSourceInfo, ingestionProperties).block();
    }

    protected abstract Mono<IngestionResult> ingestFromStreamAsyncImpl(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties);

    public Mono<IngestionResult> ingestFromStreamAsync(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) {
        // trace ingestFromStream
        return Mono.defer(() -> MonitoredActivity.wrap(
                ingestFromStreamAsyncImpl(streamSourceInfo,
                        ingestionProperties),
                getClientType().concat(".ingestFromStream")));
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
