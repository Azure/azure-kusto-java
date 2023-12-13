// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.http;

import com.azure.core.http.*;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.exceptions.*;

import org.apache.commons.lang3.StringUtils;

import org.apache.hc.core5.http.io.EofSensorInputStream;
import org.apache.hc.core5.http.io.entity.AbstractHttpEntity;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;

import java.net.URL;
import java.util.*;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

public class HttpPostUtils {
    private static final int MAX_REDIRECT_COUNT = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private HttpPostUtils() {
        // Hide constructor, as this is a static utility class
    }

    public static String post(HttpClient httpClient, String urlStr, AbstractHttpEntity body, long timeoutMs,
            Map<String, String> headers) throws DataServiceException, DataClientException {

        URL url = parseURLString(urlStr);

        HttpRequest request = setupHttpPostRequest(url, body, headers);
        int requestTimeout = timeoutMs > Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.toIntExact(timeoutMs);

        // Todo: Handle custom socket timeout settings
        // RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(requestTimeout).build();
        // request.setConfig(requestConfig);

        // Execute and get the response
        // Fixme: Send requests asynchronously instead of synchronously
        try (HttpResponse response = httpClient.sendSync(request, new Context(new Object(), null))) {
            // Fixme: Remove call to block and instead register a callback "onBody"
            String responseBody = response.getBodyAsString().block();

            if (responseBody != null) {
                switch (response.getStatusCode()) {
                    case 200:
                        return responseBody;
                    case 429:
                        throw new ThrottleException(urlStr);
                    default:
                        throw createExceptionFromResponse(urlStr, response, null, responseBody);
                }
            }
        }

        return null;
    }

    public static InputStream postToStreamingOutput(HttpClient httpClient, String url, AbstractHttpEntity entity, long timeoutMs, Map<String, String> headers)
            throws DataServiceException, DataClientException {
        return postToStreamingOutput(httpClient, url, entity, timeoutMs, headers, 0);
    }

    public static InputStream postToStreamingOutput(HttpClient httpClient, String url, AbstractHttpEntity entity, long timeoutMs, Map<String, String> headers,
            int redirectCount)
            throws DataServiceException, DataClientException {
        long timeoutTimeMs = System.currentTimeMillis() + timeoutMs;
        URL cleanedURL = parseURLString(url);

        boolean returnInputStream = false;
        String errorFromResponse = null;
        /*
         * The caller must close the inputStream to close the following underlying resources (httpClient and httpResponse). We use CloseParentResourcesStream so
         * that when the stream is closed, these resources are closed as well. We shouldn't need to do that, per
         * https://hc.apache.org/httpcomponents-client-4.5.x/current/tutorial/html/fundamentals.html: "In order to ensure proper release of system resources one
         * must close either the content stream associated with the entity or the response itself." However, in my testing this wasn't reliably the case. We
         * further use EofSensorInputStream to close the stream even if not explicitly closed, once all content is consumed.
         */
        HttpResponse httpResponse = null;
        try {
            HttpRequest httpPost = setupHttpPostRequest(cleanedURL, entity, headers);
            int requestTimeout = Math.toIntExact(timeoutTimeMs - System.currentTimeMillis());

            // Todo: Handle custom socket timeout settings
            // RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(requestTimeout).build();
            // httpPost.setConfig(requestConfig);

            // Fixme: Send requests asynchronously instead of synchronously
            httpResponse = httpClient.sendSync(httpPost, new Context(new Object(), null));

            int responseStatusCode = httpResponse.getStatusCode();

            if (responseStatusCode == 200) {
                InputStream contentStream = new EofSensorInputStream(new CloseParentResourcesStream(httpResponse), null);
                Optional<HttpHeader> contentEncoding = Optional.ofNullable(httpResponse.getHeaders().get(HttpHeaderName.CONTENT_ENCODING));
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

            // Fixme: Remove call to block and instead register a callback "onBody"
            errorFromResponse = httpResponse.getBodyAsString().block();
            // Ideal to close here (as opposed to finally) so that if any data can't be flushed, the exception will be properly thrown and handled
            httpResponse.close();

            if (shouldPostToOriginalUrlDueToRedirect(redirectCount, responseStatusCode)) {
                Optional<HttpHeader> redirectLocation = Optional.ofNullable(
                        httpResponse.getHeaders().get(HttpHeaderName.LOCATION));
                if (redirectLocation.isPresent() && !redirectLocation.get().getValue().equals(url)) {
                    return postToStreamingOutput(httpClient, redirectLocation.get().getValue(), entity, timeoutMs, headers, redirectCount + 1);
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
                    LOGGER.debug("json processing error happened while parsing errorFromResponse {}", e.getMessage(), e);
                }
            } else {
                message = String.format("Http StatusCode='%s'", httpResponse.getStatusCode());
            }

            return new DataServiceException(
                    url,
                    String.format("%s, ActivityId='%s'", message, activityId),
                    formattedException,
                    isPermanent);
        }
    }

    private static void closeResourcesIfNeeded(boolean returnInputStream, HttpResponse httpResponse) {
        // If we close the resources after returning the InputStream to the caller, they won't be able to read from it
        if (!returnInputStream) {
            if (httpResponse != null) {
                httpResponse.close();
            }
        }
    }

    private static boolean shouldPostToOriginalUrlDueToRedirect(int redirectCount, int status) {
        return (status == 302 || status == 307) && redirectCount + 1 <= MAX_REDIRECT_COUNT;
    }

    private static String determineActivityId(HttpResponse httpResponse) {
        String activityId = "";

        Optional<HttpHeader> activityIdHeader = Optional.ofNullable(
                httpResponse.getHeaders().get(HttpHeaderName.fromString("x-ms-activity-id")));
        if (activityIdHeader.isPresent()) {
            activityId = activityIdHeader.get().getValue();
        }
        return activityId;
    }

    private static HttpRequest setupHttpPostRequest(URL uri, AbstractHttpEntity entity, Map<String, String> headers) {
        HttpRequest request = new HttpRequest(HttpMethod.POST, uri);

        try {
            request.setBody(BinaryData.fromStream(entity.getContent()));
        } catch (IOException e) {
            throw new KustoParseException("Unable to generate a proper request payload from provided input.");
        }

        // Set the appropriate headers
        request.setHeader(HttpHeaderName.ACCEPT_ENCODING, "gzip,deflate");
        request.setHeader(HttpHeaderName.ACCEPT, "application/json");
        request.setHeader(HttpHeaderName.CONTENT_TYPE, "application/json");

        // TODO - maybe save this in a resouce
        String KUSTO_API_VERSION = "2019-02-13";
        request.setHeader(HttpHeaderName.fromString("x-ms-version"), KUSTO_API_VERSION);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.setHeader(HttpHeaderName.fromString(entry.getKey()), entry.getValue());
        }

        return request;
    }

    @NotNull
    private static URL parseURLString(String url) throws DataClientException {
        try {
            // By nature of the try/catch only valid URLs pass through this function
            URL cleanUrl = new URL(url);
            // Further checking here to ensure the URL uses HTTPS if not pointed at localhost
            if ("https".equalsIgnoreCase(cleanUrl.getProtocol()) || url.toLowerCase().startsWith(CloudInfo.LOCALHOST)) {
                return cleanUrl;
            } else {
                throw new DataClientException(url, "Cannot forward security token to a remote service over insecure " +
                        "channel (http://)");
            }
        } catch (MalformedURLException e) {
            throw new DataClientException(url, "Error parsing target URL in post request:" + e.getMessage(), e);
        }
    }
}
