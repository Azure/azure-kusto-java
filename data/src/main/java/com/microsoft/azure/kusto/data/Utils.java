// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.EofSensorInputStream;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

import static com.microsoft.azure.kusto.data.auth.CloudInfo.LOCALHOST;

class Utils {
    private static final int MAX_REDIRECT_COUNT = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private Utils() {
        // Hide constructor, as this is a static utility class
    }

    static String post(String url, String payload, InputStream stream, long timeoutMs, Map<String, String> headers, boolean leaveOpen) throws DataServiceException, DataClientException {
        URI uri = parseUriFromUrlString(url);

        HttpClient httpClient = getHttpClient(timeoutMs > Integer.MAX_VALUE ?
                Integer.MAX_VALUE :
                Math.toIntExact(Integer.MAX_VALUE));

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
                    throw createExceptionFromResponse(url, response, null, responseContent);
                }
            }
        }
        catch (SocketTimeoutException e) {
            throw new DataServiceException(url, "Timed out in post request:" + e.getMessage(), false);
        }
        catch (JSONException | IOException e) {
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
        boolean returnInputStream = false;
        String errorFromResponse = null;
        /*
         *  The caller must close the inputStream to close the following underlying resources (httpClient and httpResponse).
         *  We use CloseParentResourcesStream so that when the stream is closed, these resources are closed as well. We
         *  shouldn't need to do that, per https://hc.apache.org/httpcomponents-client-4.5.x/current/tutorial/html/fundamentals.html:
         *  "In order to ensure proper release of system resources one must close either the content stream associated
         *  with the entity or the response itself." However, in my testing this wasn't reliably the case.
         *  We further use EofSensorInputStream to close the stream even if not explicitly closed, once all content is consumed.
         */
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse httpResponse = null;
        try {
            httpClient = getHttpClient(Math.toIntExact(timeoutTimeMs - System.currentTimeMillis()));
            httpResponse = httpClient.execute(setupHttpPostRequest(uri, payload, null, headers));

            int responseStatusCode = httpResponse.getStatusLine().getStatusCode();

            if (responseStatusCode == HttpStatus.SC_OK) {
                InputStream contentStream = new EofSensorInputStream(new CloseParentResourcesStream(httpClient, httpResponse), null);
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
                // Though the server responds with a gzip/deflate Content-Encoding header, we reach here because httpclient uses LazyDecompressingStream which handles the above logic
                returnInputStream = true;
                return contentStream;
            }

            errorFromResponse = EntityUtils.toString(httpResponse.getEntity());
            // Ideal to close here (as opposed to finally) so that if any data can't be flushed, the exception will be properly thrown and handled
            httpResponse.close();
            httpClient.close();

            if (shouldPostToOriginalUrlDueToRedirect(redirectCount, responseStatusCode)) {
                Optional<Header> redirectLocation = Arrays.stream(httpResponse.getHeaders(HttpHeaders.LOCATION)).findFirst();
                if (redirectLocation.isPresent() && !redirectLocation.get().getValue().equals(url)) {
                    return postToStreamingOutput(redirectLocation.get().getValue(), payload, timeoutMs, headers, redirectCount + 1);
                }
            }
        } catch (IOException ex) {
            throw new DataServiceException(url, "postToStreamingOutput failed to get or decompress response stream",
                    ex, false);
        } catch (Exception ex) {
            throw createExceptionFromResponse(url, httpResponse, ex, errorFromResponse);
        } finally {
            closeResourcesIfNeeded(returnInputStream, httpClient, httpResponse);
        }

        throw createExceptionFromResponse(url, httpResponse, null, errorFromResponse);
    }

    private static DataServiceException createExceptionFromResponse(String url, HttpResponse httpResponse, Exception thrownException, String errorFromResponse) {
        if (httpResponse == null) {
            return new DataServiceException(url, "POST failed to send request", thrownException, false);
        } else {
            /*
             *  TODO: When we add another streaming API that returns a KustoOperationResult, we'll need to handle the 2 types of
             *   content errors this API can return: (1) Inline error (engine identifies error after it starts building the json
             *   result), or (2) in the KustoOperationResult's QueryCompletionInformation, both of which present with "200 OK". See .Net's DataReaderParser.
             */
            String activityId = determineActivityId(httpResponse);
            if (!StringUtils.isBlank(errorFromResponse)) {
                String message = "";
                DataWebException formattedException = new DataWebException(errorFromResponse, httpResponse);
                try {
                    message = String.format("%s, ActivityId='%s'", formattedException.getApiError().getDescription(), activityId);
                    return new DataServiceException(url, message, formattedException,
                            formattedException.getApiError().isPermanent());
                } catch (Exception ignored) {
                }
            }
            errorFromResponse = String.format("Http StatusCode='%s', ActivityId='%s'", httpResponse.getStatusLine().toString(), activityId);
            return new DataServiceException(url, errorFromResponse, thrownException, false);

        }
    }

    private static void closeResourcesIfNeeded(boolean returnInputStream, CloseableHttpClient httpClient, CloseableHttpResponse httpResponse) {
        // If we close the resources after returning the InputStream to the caller, they won't be able to read from it
        if (!returnInputStream) {
            try {
                if (httpResponse != null) {
                    httpResponse.close();
                }
                if (httpClient != null) {
                    httpClient.close();
                }
            } catch (IOException ex) {
                // There's nothing we can do if the resources can't be closed, and we don't want to add an exception to the signature
                LOGGER.error("Couldn't close HttpClient resources. This won't affect the POST call, but should be investigated.");
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

    /*
     *  TODO: Per the Apache HttpClient docs: "Generally it is recommended to have a single instance of HttpClient per
     *  communication component or even per application. However, if the application makes use of HttpClient only very
     *  infrequently, and keeping an idle instance of HttpClient in memory is not warranted, it is highly recommended to
     *  explicitly shut down the multithreaded connection manager prior to disposing the HttpClient instance. This will
     *  ensure proper closure of all HTTP connections in the connection pool."
     *  I'll add as an issue for a future enhancement that both POST methods should reuse the HttpClient via Factory,
     *  because it can be created with a specified timeout, and we'd need to create an HttpClient per-timeout.
     */
    private static CloseableHttpClient getHttpClient(int timeoutMs) {
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeoutMs).build();
        return HttpClientBuilder.create().useSystemProperties().setDefaultRequestConfig(requestConfig).build();
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
            if ("https".equalsIgnoreCase(cleanUrl.getProtocol()) || url.equalsIgnoreCase(LOCALHOST)) {
                return new URI(cleanUrl.getProtocol(), cleanUrl.getUserInfo(), cleanUrl.getHost(), cleanUrl.getPort(), cleanUrl.getPath(), cleanUrl.getQuery(), cleanUrl.getRef());
            } else {
//                throw new DataClientException(url, "Cannot forward security token to a remote service over insecure " +
//                        "channel (http://)");
                return new URI(cleanUrl.getProtocol(), cleanUrl.getUserInfo(), cleanUrl.getHost(), cleanUrl.getPort(), cleanUrl.getPath(), cleanUrl.getQuery(), cleanUrl.getRef());
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
    static final int SECONDS_PER_MINUTE = 60;
    static final int MINUTES_PER_HOUR = 60;
    static final int HOURS_PER_DAY = 24;
    static final int SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR;
    static final int SECONDS_PER_DAY = HOURS_PER_DAY * SECONDS_PER_HOUR;

    public static String formatDurationAsTimespan(Duration duration) {
        long seconds = duration.getSeconds();
        int nanos = duration.getNano();

        long hours = (seconds / SECONDS_PER_HOUR) % HOURS_PER_DAY;
        long minutes = ((seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
        long secs = (seconds % SECONDS_PER_MINUTE);
        long days = (seconds / SECONDS_PER_DAY);
        String positive = String.format(
                "%02d.%02d:%02d:%02d.%.3s",
                days,
                hours,
                minutes,
                secs,
                nanos);

        return seconds < 0 ? "-" + positive : positive;
    }
}