package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;

public abstract class IngestClientBase {
    static final String INGEST_PREFIX = "://ingest-";
    static final String NO_INGEST_PREFIX = "://";

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    String connectionDataSource;
    protected static final String WRONG_ENDPOINT_MESSAGE = "Ingestion failed likely because the wrong endpoint, whose ServiceType %s, was configured, which isn't compatible with the client of type '%s' being used. Initialize the client with the appropriate endpoint URL";

    static boolean shouldCompress(CompressionType sourceCompressionType, IngestionProperties.DataFormat dataFormat) {
        return (sourceCompressionType == null) && (dataFormat == null || dataFormat.isCompressible());
    }

    static String getIngestionEndpoint(String clusterUrl) {
        if (clusterUrl.contains(INGEST_PREFIX)) {
            return clusterUrl;
        } else {
            return clusterUrl.replace(NO_INGEST_PREFIX, INGEST_PREFIX);
        }
    }

    static String getQueryEndpoint(String clusterUrl) {
        if (clusterUrl.contains(INGEST_PREFIX)) {
            return clusterUrl.replace(INGEST_PREFIX, NO_INGEST_PREFIX);
        } else {
            return clusterUrl;
        }
    }

}
