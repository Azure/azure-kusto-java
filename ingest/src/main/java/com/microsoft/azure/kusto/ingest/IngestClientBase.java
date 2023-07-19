package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import org.apache.http.conn.util.InetAddressUtils;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

public abstract class IngestClientBase {
    static final String INGEST_PREFIX = "ingest-";
    static final String PROTOCOL_SUFFIX = "://";

    String connectionDataSource;

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
        return isReservedHostname(clusterUrl) ? clusterUrl : clusterUrl.replaceFirst(INGEST_PREFIX, "");
    }

    static boolean isReservedHostname(String rawUri) {
        URI uri = URI.create(rawUri);
        if (!uri.isAbsolute()) {
            return true;
        }
        var authority = uri.getAuthority().toLowerCase();
        var isIPFlag = InetAddressUtils.isIPv4Address(authority) || InetAddressUtils.isIPv6Address(authority);
        var isLocalFlagg = false;
        try {
            isLocalFlagg = InetAddress.getByName(authority).isLoopbackAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        return isLocalFlagg || isIPFlag || authority.equalsIgnoreCase("onebox.dev.kusto.windows.net");
    }
}
