// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.EofSensorInputStream;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

class Utils {
    private static final int MAX_REDIRECT_COUNT = 1;

    private Utils() {
        // Hide constructor, as this is a static utility class
    }

    static String post(String url, String payload, InputStream stream, Integer timeoutMs, Map<String, String> headers, boolean leaveOpen) throws DataServiceException, DataClientException {
        URI uri = parseUriFromUrlString(url);

        HttpClient httpClient;
        if (timeoutMs != null) {
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeoutMs).build();
            httpClient = HttpClientBuilder.create().useSystemProperties().setDefaultRequestConfig(requestConfig).build();
        } else {
            httpClient = HttpClients.createSystem();
        }

        try (InputStream ignored = (stream != null && !leaveOpen) ? stream : null) {
            HttpPost request = setupHttpPostRequest(uri, payload, stream, headers);

            // Execute and get the response
            HttpResponse response = httpClient.execute(request);

            HttpEntity entity = response.getEntity();
            if (entity != null) {
                StatusLine statusLine = response.getStatusLine();
                String responseContent = EntityUtils.toString(entity);
                if (statusLine.getStatusCode() == 200) {
                    return responseContent;
                } else {
                    if (StringUtils.isBlank(responseContent)) {
                        responseContent = response.getStatusLine().toString();
                    }
                    String message = "";
                    DataWebException ex = new DataWebException(responseContent, response);
                    try {
                        message = ex.getApiError().getDescription();
                    } catch (Exception ignored1) {
                    }
                    throw new DataServiceException(url, message, ex);
                }
            }
        } catch (JSONException | IOException e) {
            throw new DataClientException(url, "Error in post request:" + e.getMessage(), e);
        }
        return null;
    }

    static InputStream postToStreamingOutput(String url, String payload, long timeoutMs, Map<String, String> headers) throws DataServiceException, DataClientException {
        return postToStreamingOutput(url, payload, timeoutMs, headers, 0);
    }

    static InputStream postToStreamingOutput(String url, String payload, long timeoutMs, Map<String, String> headers, int redirectCount) throws DataServiceException, DataClientException {
        long timeoutTimeMs = System.currentTimeMillis() + timeoutMs;
        URI uri = parseUriFromUrlString(url);
        /*
         *  The caller must close the inputStream to close the following underlying resources (httpClient and httpResponse).
         *  We use CloseParentResourcesStream so that when the stream is closed, these resources are closed as well. We
         *  shouldn't need to do that, per https://hc.apache.org/httpcomponents-client-4.5.x/current/tutorial/html/fundamentals.html:
         *  "In order to ensure proper release of system resources one must close either the content stream associated
         *  with the entity or the response itself." However, in my testing this wasn't reliably the case.
         *  We further use EofSensorInputStream to close the stream even if not explicitly closed, once all content is consumed.
         */
        CloseableHttpResponse httpResponse;
        CloseableHttpClient httpClient = getHttpClient(timeoutTimeMs - System.currentTimeMillis());
        try {
            httpResponse = httpClient.execute(setupHttpPostRequest(uri, payload, null, headers));

            int status = httpResponse.getStatusLine().getStatusCode();

            if (status == HttpStatus.SC_OK) {
                InputStream contentStream = new CloseParentResourcesStream(new EofSensorInputStream(httpResponse.getEntity().getContent(), null), httpClient, httpResponse);
                Optional<Header> contentEncoding = Arrays.stream(httpResponse.getHeaders(HttpHeaders.CONTENT_ENCODING)).findFirst();
                if (contentEncoding.isPresent()) {
                    if (contentEncoding.get().getValue().contains("gzip")) {
                        return new GZIPInputStream(contentStream);
                    } else if (contentEncoding.get().getValue().contains("deflate")) {
                        return new DeflaterInputStream(contentStream);
                    }
                }
                // Though the server responds with a gzip/deflate Content-Encoding header, we reach here because httpclient uses LazyDecompressingStream which handles the above logic
                return contentStream;
            } else if ((status == HttpStatus.SC_MOVED_TEMPORARILY || status == HttpStatus.SC_TEMPORARY_REDIRECT)
                    && redirectCount + 1 <= MAX_REDIRECT_COUNT) {
                Optional<Header> redirectLocation = Arrays.stream(httpResponse.getHeaders(HttpHeaders.LOCATION)).findFirst();
                if (redirectLocation.isPresent() && !redirectLocation.get().getValue().equals(url)) {
                    return postToStreamingOutput(redirectLocation.get().getValue(), payload, timeoutMs, headers, redirectCount + 1);
                }
            }
        } catch (IOException ex) {
            throw new DataServiceException("postToStreamingOutput failed to get or decompress response stream for url=" + url, ex);
        } catch (Exception ex) {
            throw new DataServiceException("postToStreamingOutput failed to send request to url=" + url, ex);
        }

        /*
         *  TODO: When we add another streaming API that returns a KustoOperationResult, we'll need to handle the 2 types of
         *   content errors this API can return: (1) Inline error (engine identifies error after it starts building the json
         *   result), or (2) in the KustoOperationResult's QueryCompletionInformation, both of which present with "200 OK". See .Net's DataReaderParser.
         */
        String activityId = determineActivityId(httpResponse);
        throw new DataServiceException(String.format("Didn't receive successful response to streaming query request. Response Code = '%s', ActivityId = '%s'", httpResponse.getStatusLine().getStatusCode(), activityId));
    }

    private static String determineActivityId(HttpResponse httpResponse) {
        String activityId = "";
        Optional<Header> activityIdHeader = Arrays.stream(httpResponse.getHeaders("x-ms-activity-id")).findFirst();
        if (activityIdHeader.isPresent()) {
            activityId = activityIdHeader.get().getValue();
        }
        return activityId;
    }

    private static CloseableHttpClient getHttpClient(Long timeoutMs) {
        if (timeoutMs != null) {
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeoutMs.intValue()).build();
            return HttpClientBuilder.create().useSystemProperties().setDefaultRequestConfig(requestConfig).build();
        } else {
            return HttpClients.createSystem();
        }
    }

    private static HttpPost setupHttpPostRequest(URI uri, String payload, InputStream stream, Map<String, String> headers) {
        HttpPost request = new HttpPost(uri);

        // Request parameters and other properties. We use UncloseableStream to prevent HttpClient From closing it
        HttpEntity requestEntity = (stream == null) ? new StringEntity(payload, ContentType.APPLICATION_JSON)
                : new InputStreamEntity(new UncloseableStream(stream));
        request.setEntity(requestEntity);

        request.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip,deflate");
        request.addHeader(HttpHeaders.ACCEPT, "application/json");
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }
        return request;
    }

    @NotNull
    private static URI parseUriFromUrlString(String url) throws DataClientException {
        try {
            URL cleanUrl = new URL(url);
            if ("https".equalsIgnoreCase(cleanUrl.getProtocol())) {
                return new URI(cleanUrl.getProtocol(), cleanUrl.getUserInfo(), cleanUrl.getHost(), cleanUrl.getPort(), cleanUrl.getPath(), cleanUrl.getQuery(), cleanUrl.getRef());
            } else {
                throw new DataClientException("Cannot forward security token to a remote service over insecure channel (http://)");
            }
        } catch (URISyntaxException | MalformedURLException e) {
            throw new DataClientException(url, "Error parsing target URL in post request:" + e.getMessage(), e);
        }
    }

    static String getPackageVersion() {
        try {
            Properties props = new Properties();
            try (InputStream versionFileStream = Utils.class.getResourceAsStream("/app.properties")) {
                props.load(versionFileStream);
                return props.getProperty("version").trim();
            }
        } catch (Exception ignored) {
        }
        return "";
    }
}