// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IngestClientBase {
    private static final Logger log =
            LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    String connectionDataSource;
    private String endpointServiceType;
    private String suggestedEndpointUri;
    public static final String INGEST_PREFIX = "ingest-";
    protected static final String WRONG_ENDPOINT_MESSAGE =
            "You are using '%s' client type, but the provided endpoint is of ServiceType '%s'. Initialize the client with the appropriate endpoint URI";

    protected void validateEndpointServiceType(String connectionDataSource, String expectedServiceType)
            throws IngestionServiceException, IngestionClientException {
        if (StringUtils.isBlank(endpointServiceType)) {
            endpointServiceType = retrieveServiceType();
        }
        if (!expectedServiceType.equals(endpointServiceType)) {
            String message = String.format(WRONG_ENDPOINT_MESSAGE, expectedServiceType, endpointServiceType);
            suggestedEndpointUri = generateEndpointSuggestion(suggestedEndpointUri, connectionDataSource);
            if (StringUtils.isNotBlank(suggestedEndpointUri)) {
                message = String.format("%s: '%s'", message, suggestedEndpointUri);
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
                log.error("Couldn't parse dataSource '{}', so no suggestion can be made.", dataSource, e);
            } catch (IllegalArgumentException e) {
                log.error("URL is already in the correct format '{}', so no suggestion can be made.", dataSource, e);
            }
        }

        return endpointUriToSuggestStr;
    }

    protected abstract String retrieveServiceType() throws IngestionServiceException, IngestionClientException;

    protected abstract String emendEndpointUri(URIBuilder existingEndpoint);

    static boolean shouldCompress(CompressionType sourceCompressionType, IngestionProperties.DataFormat dataFormat) {
        return (sourceCompressionType == null) && (dataFormat == null || dataFormat.isCompressible());
    }
}
