package com.microsoft.azure.kusto.data;

import com.azure.core.http.*;
import com.azure.core.util.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.azure.kusto.data.exceptions.*;
import com.microsoft.azure.kusto.data.http.CloseParentResourcesStream;
import com.microsoft.azure.kusto.data.http.HttpRequestBuilder;
import com.microsoft.azure.kusto.data.http.HttpStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.EofSensorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public abstract class BaseClient implements Client, StreamingClient {

    private static final int MAX_REDIRECT_COUNT = 1;
    private static final int EXTRA_TIMEOUT_FOR_CLIENT_SIDE = (int) TimeUnit.SECONDS.toMillis(30);

    // Make logger available to implementations
    protected static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final HttpClient httpClient;

    public BaseClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    protected String post(HttpRequest request, long timeoutMs) throws DataServiceException {
        // Execute and get the response
        try (HttpResponse response = httpClient.sendSync(request, getContextTimeout(timeoutMs))) {
            return processResponseBody(response);
        } catch (DataServiceException e) {
            throw e;
        } catch (Exception e) {
            throw ExceptionUtils.createExceptionOnPost(e, request.getUrl(), "sync");
        }
    }

    private String processResponseBody(HttpResponse response) throws DataServiceException {
        String responseBody = Utils.isGzipResponse(response) ? Utils.gzipedInputToString(response.getBodyAsBinaryData().toStream())
                : response.getBodyAsBinaryData().toString();

        if (responseBody == null) {
            return null;
        }

        switch (response.getStatusCode()) {
            case HttpStatus.OK:
                return responseBody;
            case HttpStatus.TOO_MANY_REQS:
                throw new ThrottleException(response.getRequest().getUrl().toString());
            default:
                throw createExceptionFromResponse(response.getRequest().getUrl().toString(), response, null, responseBody);
        }
    }

    // Todo: Implement async version of this method
    protected InputStream postToStreamingOutput(HttpRequest request, long timeoutMs, int currentRedirectCounter, int maxRedirectCount)
            throws DataServiceException {
        boolean returnInputStream = false;
        String errorFromResponse = null;

        HttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.sendSync(request, getContextTimeout(timeoutMs));

            int responseStatusCode = httpResponse.getStatusCode();

            if (responseStatusCode == HttpStatus.OK) {
                returnInputStream = true;
                return new EofSensorInputStream(new CloseParentResourcesStream(httpResponse), null);
            }

            errorFromResponse = httpResponse.getBodyAsBinaryData().toString();
            // Ideal to close here (as opposed to finally) so that if any data can't be flushed, the exception will be properly thrown and handled
            httpResponse.close();

            if (shouldPostToOriginalUrlDueToRedirect(responseStatusCode, currentRedirectCounter, maxRedirectCount)) {
                Optional<HttpHeader> redirectLocation = Optional.ofNullable(httpResponse.getHeaders().get(HttpHeaderName.LOCATION));
                if (redirectLocation.isPresent() && !redirectLocation.get().getValue().equals(request.getUrl().toString())) {
                    HttpRequest redirectRequest = HttpRequestBuilder
                            .fromExistingRequest(request)
                            .withURL(redirectLocation.get().getValue())
                            .build();
                    return postToStreamingOutput(redirectRequest, timeoutMs, currentRedirectCounter + 1, maxRedirectCount);
                }
            }
        } catch (IOException ex) {
            // Thrown from new CloseParentResourcesStream(httpResponse)
            throw new DataServiceException(request.getUrl().toString(),
                    "postToStreamingOutput failed to get or decompress response stream", ex, false);
        } catch (UncheckedIOException e) {
            throw ExceptionUtils.createExceptionOnPost(e, request.getUrl(), "streaming sync");
        } catch (Exception ex) {
            throw createExceptionFromResponse(request.getUrl().toString(), httpResponse, ex, errorFromResponse);
        } finally {
            closeResourcesIfNeeded(returnInputStream, httpResponse);
        }

        throw createExceptionFromResponse(request.getUrl().toString(), httpResponse, null, errorFromResponse);
    }

    public static DataServiceException createExceptionFromResponse(String url, HttpResponse httpResponse, Exception thrownException, String errorFromResponse) {
        if (httpResponse == null) {
            return new DataServiceException(url, "POST failed to send request", thrownException, false);
        }
        /*
         * TODO: When we add another streaming API that returns a KustoOperationResult, we'll need to handle the 2 types of content errors this API can return:
         * (1) Inline error (engine identifies error after it starts building the json result), or (2) in the KustoOperationResult's QueryCompletionInformation,
         * both of which present with "200 OK". See .Net's DataReaderParser.
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

    private static Context getContextTimeout(long timeoutMs) {
        int requestTimeout = timeoutMs > Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.toIntExact(timeoutMs) + EXTRA_TIMEOUT_FOR_CLIENT_SIDE;

        // See https://github.com/Azure/azure-sdk-for-java/blob/azure-core-http-netty_1.10.2/sdk/core/azure-core-http-netty/CHANGELOG.md#features-added
        return Context.NONE.addData("azure-response-timeout", Duration.ofMillis(requestTimeout));
    }

    private static void closeResourcesIfNeeded(boolean returnInputStream, HttpResponse httpResponse) {
        // If we close the resources after returning the InputStream to the user, he won't be able to read from it - used in streaming query
        if (!returnInputStream) {
            if (httpResponse != null) {
                httpResponse.close();
            }
        }
    }

    private static boolean shouldPostToOriginalUrlDueToRedirect(int status, int redirectCount, int maxRedirectCount) {
        return (status == HttpStatus.FOUND || status == HttpStatus.TEMP_REDIRECT) && redirectCount + 1 <= maxRedirectCount;
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

}
