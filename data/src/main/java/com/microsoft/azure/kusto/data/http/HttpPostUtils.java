// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.http;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.exceptions.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.EofSensorInputStream;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.net.*;
import java.util.*;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

public class HttpPostUtils {
    private static final int MAX_REDIRECT_COUNT = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private HttpPostUtils() {
        // Hide constructor, as this is a static utility class
    }

    public static String post(CloseableHttpClient httpClient, String urlStr, AbstractHttpEntity requestEntity, long timeoutMs,
            Map<String, String> headers)
            throws DataServiceException, DataClientException {
        URI url = parseUriFromUrlString(urlStr);

        try {
            HttpPost request = setupHttpPostRequest(url, requestEntity, headers);
            int requestTimeout = timeoutMs > Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.toIntExact(timeoutMs);
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(requestTimeout).build();
            request.setConfig(requestConfig);

            // Execute and get the response
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                HttpEntity entity = response.getEntity();

                if (entity != null) {
                    StatusLine statusLine = response.getStatusLine();
                    String responseContent = EntityUtils.toString(entity);
                    switch (statusLine.getStatusCode()) {
                        case HttpStatus.SC_OK:
                            return responseContent;
                        case HttpStatus.SC_TOO_MANY_REQUESTS:
                            throw new ThrottleException(urlStr);
                        default:
                            throw createExceptionFromResponse(urlStr, response, null, responseContent);
                    }
                }
            }
        } catch (IOException e) {
            throw new DataServiceException(urlStr, "IOException in post request:" + e.getMessage(), !Utils.isRetriableIOException(e));
        }

        return null;
    }

    public static InputStream postToStreamingOutput(CloseableHttpClient httpClient, String url, String payload, long timeoutMs, Map<String, String> headers)
            throws DataServiceException, DataClientException {
        return postToStreamingOutput(httpClient, url, payload, timeoutMs, headers, 0);
    }

    public static InputStream postToStreamingOutput(CloseableHttpClient httpClient, String url, String payload, long timeoutMs, Map<String, String> headers,
                                             int redirectCount)
            throws DataServiceException, DataClientException {
        long timeoutTimeMs = System.currentTimeMillis() + timeoutMs;
        URI uri = parseUriFromUrlString(url);

        boolean returnInputStream = false;
        String errorFromResponse = null;
        /*
         * The caller must close the inputStream to close the following underlying resources (httpClient and httpResponse). We use CloseParentResourcesStream so
         * that when the stream is closed, these resources are closed as well. We shouldn't need to do that, per
         * https://hc.apache.org/httpcomponents-client-4.5.x/current/tutorial/html/fundamentals.html: "In order to ensure proper release of system resources one
         * must close either the content stream associated with the entity or the response itself." However, in my testing this wasn't reliably the case. We
         * further use EofSensorInputStream to close the stream even if not explicitly closed, once all content is consumed.
         */
        CloseableHttpResponse httpResponse = null;
        try {
            StringEntity requestEntity = new StringEntity(payload, ContentType.APPLICATION_JSON);
            HttpPost httpPost = setupHttpPostRequest(uri, requestEntity, headers);
            int requestTimeout = Math.toIntExact(timeoutTimeMs - System.currentTimeMillis());
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(requestTimeout).build();
            httpPost.setConfig(requestConfig);

            httpResponse = httpClient.execute(httpPost);

            int responseStatusCode = httpResponse.getStatusLine().getStatusCode();

            if (responseStatusCode == HttpStatus.SC_OK) {
                InputStream contentStream = new EofSensorInputStream(new CloseParentResourcesStream(httpResponse), null);
                Optional<Header> contentEncoding = Arrays.stream(httpResponse.getHeaders(HttpHeaders.CONTENT_ENCODING)).findFirst();
                if (contentEncoding.isPresent()) {
                    if (contentEncoding.get().getValue().contains("gzip")) {
                        GZIPInputStream gzipInputStream = new GZIPInputStream(contentStream);
                        returnInputStream = true;
                        return gzipInputStream;
                    } else if (contentEncoding.get().getValue().contains("deflate")) {
                        DeflaterInputStream deflaterInputStream = new DeflaterInputStream(contentStream);
                        returnInputStream = true;
                        return deflaterInputStream;
                    }
                }
                // Though the server responds with a gzip/deflate Content-Encoding header, we reach here because httpclient uses LazyDecompressingStream which
                // handles the above logic
                returnInputStream = true;
                return contentStream;
            }

            errorFromResponse = EntityUtils.toString(httpResponse.getEntity());
            // Ideal to close here (as opposed to finally) so that if any data can't be flushed, the exception will be properly thrown and handled
            httpResponse.close();

            if (shouldPostToOriginalUrlDueToRedirect(redirectCount, responseStatusCode)) {
                Optional<Header> redirectLocation = Arrays.stream(httpResponse.getHeaders(HttpHeaders.LOCATION)).findFirst();
                if (redirectLocation.isPresent() && !redirectLocation.get().getValue().equals(url)) {
                    return postToStreamingOutput(httpClient, redirectLocation.get().getValue(), payload, timeoutMs, headers, redirectCount + 1);
                }
            }
        } catch (IOException ex) {
            throw new DataServiceException(url, "postToStreamingOutput failed to get or decompress response stream",
                    ex, false);
        } catch (Exception ex) {
            throw createExceptionFromResponse(url, httpResponse, ex, errorFromResponse);
        } finally {
            closeResourcesIfNeeded(returnInputStream, httpResponse);
        }

        throw createExceptionFromResponse(url, httpResponse, null, errorFromResponse);
    }

    public static DataServiceException createExceptionFromResponse(String url, HttpResponse httpResponse, Exception thrownException, String errorFromResponse) {
        if (httpResponse == null) {
            return new DataServiceException(url, "POST failed to send request", thrownException, false);
        } else {
            /*
             * TODO: When we add another streaming API that returns a KustoOperationResult, we'll need to handle the 2 types of content errors this API can
             * return: (1) Inline error (engine identifies error after it starts building the json result), or (2) in the KustoOperationResult's
             * QueryCompletionInformation, both of which present with "200 OK". See .Net's DataReaderParser.
             */
            String activityId = determineActivityId(httpResponse);
            String message = errorFromResponse;
            WebException formattedException = new WebException(errorFromResponse, httpResponse, thrownException);
            boolean isPermanent = false;
            if (!StringUtils.isBlank(errorFromResponse)) {
                try {
                    JsonNode jsonObject = Utils.getObjectMapper().readTree(errorFromResponse);
                    if (jsonObject.has("error")) {
                        formattedException = new DataWebException(errorFromResponse, httpResponse, thrownException);
                        OneApiError apiError = ((DataWebException) formattedException).getApiError();
                        message = apiError.getDescription();
                        isPermanent = apiError.isPermanent();
                    } else if (jsonObject.has("message")) {
                        message = jsonObject.get("message").asText();
                    }
                } catch (JsonProcessingException e) {
                    // It's not ideal to use an exception here for control flow, but we can't know if it's a valid JSON until we try to parse it
                    LOGGER.debug("json processing error happened while parsing errorFromResponse {}" + e.getMessage(), e);
                }
            } else {
                message = String.format("Http StatusCode='%s'", httpResponse.getStatusLine().toString());
            }

            return new DataServiceException(
                    url,
                    String.format("%s, ActivityId='%s'", message, activityId),
                    formattedException,
                    isPermanent);
        }
    }

    private static void closeResourcesIfNeeded(boolean returnInputStream, CloseableHttpResponse httpResponse) {
        // If we close the resources after returning the InputStream to the caller, they won't be able to read from it
        if (!returnInputStream) {
            try {
                if (httpResponse != null) {
                    httpResponse.close();
                }
            } catch (IOException ex) {
                // There's nothing we can do if the resources can't be closed, and we don't want to add an exception to the signature
                LOGGER.error("Couldn't close HttpResponse. This won't affect the POST call, but should be investigated.");
            }
        }
    }

    private static boolean shouldPostToOriginalUrlDueToRedirect(int redirectCount, int status) {
        return (status == HttpStatus.SC_MOVED_TEMPORARILY || status == HttpStatus.SC_TEMPORARY_REDIRECT)
                && redirectCount + 1 <= MAX_REDIRECT_COUNT;
    }

    private static String determineActivityId(HttpResponse httpResponse) {
        String activityId = "";
        Optional<Header> activityIdHeader = Arrays.stream(httpResponse.getHeaders("x-ms-activity-id")).findFirst();
        if (activityIdHeader.isPresent()) {
            activityId = activityIdHeader.get().getValue();
        }
        return activityId;
    }

    private static HttpPost setupHttpPostRequest(URI uri, AbstractHttpEntity requestEntity, Map<String, String> headers) {
        HttpPost request = new HttpPost(uri);

        // Request parameters and other properties
        request.setEntity(requestEntity);

        request.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip,deflate");
        request.addHeader(HttpHeaders.ACCEPT, "application/json");

        // TODO - maybe save this in a resouce
        String KUSTO_API_VERSION = "2019-02-13";
        request.addHeader("x-ms-version", KUSTO_API_VERSION);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }

        return request;
    }

    @NotNull
    private static URI parseUriFromUrlString(String url) throws DataClientException {
        try {
            URL cleanUrl = new URL(url);
            if ("https".equalsIgnoreCase(cleanUrl.getProtocol()) || url.toLowerCase().startsWith(CloudInfo.LOCALHOST)) {
                return new URI(cleanUrl.getProtocol(), cleanUrl.getUserInfo(), cleanUrl.getHost(), cleanUrl.getPort(), cleanUrl.getPath(), cleanUrl.getQuery(),
                        cleanUrl.getRef());
            } else {
                throw new DataClientException(url, "Cannot forward security token to a remote service over insecure " +
                        "channel (http://)");
            }
        } catch (URISyntaxException | MalformedURLException e) {
            throw new DataClientException(url, "Error parsing target URL in post request:" + e.getMessage(), e);
        }
    }
}
