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
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    String connectionDataSource;
    private String endpointServiceType;
    private String suggestedEndpointUri;
    public static final String INGEST_PREFIX = "ingest-";
    protected static final String WRONG_ENDPOINT_MESSAGE = "Ingestion failed likely because the wrong endpoint, whose ServiceType %s, was configured, which isn't compatible with the client of type '%s' being used. Initialize the client with the appropriate endpoint URL";
    protected static final String CONFIGURED_ENDPOINT_MESSAGE = "is '%s'";
    protected static final String INDETERMINATE_CONFIGURED_ENDPOINT_MESSAGE = "couldn't be determined";

    protected void validateEndpointServiceType(String connectionDataSource, String expectedServiceType)
        throws IngestionClientException {
        if (StringUtils.isBlank(endpointServiceType)) {
            endpointServiceType = retrieveServiceType();
        }
        if (!expectedServiceType.equals(endpointServiceType)) {
            String message;
            if (StringUtils.isNotBlank(endpointServiceType)) {
                String configuredEndpointMessage = String.format(CONFIGURED_ENDPOINT_MESSAGE, endpointServiceType);
                message = String.format(WRONG_ENDPOINT_MESSAGE, configuredEndpointMessage, expectedServiceType);
            } else {
                message = String.format(WRONG_ENDPOINT_MESSAGE, INDETERMINATE_CONFIGURED_ENDPOINT_MESSAGE, expectedServiceType);
            }
            suggestedEndpointUri = generateEndpointSuggestion(suggestedEndpointUri, connectionDataSource);
            if (StringUtils.isNotBlank(suggestedEndpointUri)) {
                message = String.format("%s, which is likely '%s'.", message, suggestedEndpointUri);
            } else {
                message += ".";
            }
            throw new IngestionClientException(message);
        }
    }

    protected String generateEndpointSuggestion(String existingSuggestedEndpointUri, String dataSource) {
        if (existingSuggestedEndpointUri != null) {
            return existingSuggestedEndpointUri;
        }
        // The default is not passing a suggestion to the exception
        String endpointUriToSuggestStr = "";
        if (StringUtils.isNotBlank(dataSource)) {
            URIBuilder existingEndpoint;
            try {
                existingEndpoint = new URIBuilder(dataSource);
                endpointUriToSuggestStr = emendEndpointUri(existingEndpoint);
            } catch (URISyntaxException e) {
                log.warn("Couldn't parse dataSource '{}', so no suggestion can be made.", dataSource, e);
            } catch (IllegalArgumentException e) {
                log.warn("URL is already in the correct format '{}', so no suggestion can be made.", dataSource, e);
            }
        }

        return endpointUriToSuggestStr;
    }

    protected abstract String retrieveServiceType();

    protected abstract String emendEndpointUri(URIBuilder existingEndpoint);

    static boolean shouldCompress(CompressionType sourceCompressionType, IngestionProperties.DataFormat dataFormat) {
        return (sourceCompressionType == null) && (dataFormat == null || dataFormat.isCompressible());
    }
}
