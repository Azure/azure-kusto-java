package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.ingest.source.CompressionType;

import java.net.URI;
import java.util.regex.Pattern;

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
        var host = uri.getHost().toLowerCase();
        Pattern IPV4_PATTERN = Pattern.compile("^(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)(\\.(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)){3}$");

        Pattern IPV6_STD_PATTERN = Pattern.compile("^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$");

        Pattern IPV6_HEX_COMPRESSED_PATTERN = Pattern.compile("^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$");

        var isIPFlag = IPV4_PATTERN.matcher(host).matches() || IPV6_STD_PATTERN.matcher(host).matches() || IPV6_HEX_COMPRESSED_PATTERN.matcher(host).matches();

        return UriUtils.isLocalAddress(host) || isIPFlag || host.equalsIgnoreCase("onebox.dev.kusto.windows.net");
    }
}
