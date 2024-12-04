package com.microsoft.azure.kusto.data;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeader;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.util.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import com.microsoft.azure.kusto.data.exceptions.ExceptionUtils;
import com.microsoft.azure.kusto.data.exceptions.OneApiError;
import com.microsoft.azure.kusto.data.exceptions.ThrottleException;
import com.microsoft.azure.kusto.data.exceptions.WebException;
import com.microsoft.azure.kusto.data.http.CloseParentResourcesStream;
import com.microsoft.azure.kusto.data.http.HttpRequestBuilder;
import com.microsoft.azure.kusto.data.http.HttpStatus;
import com.microsoft.azure.kusto.data.req.RequestUtils;
import com.microsoft.azure.kusto.data.res.ResponseState;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.EofSensorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public abstract class BaseClient implements Client, StreamingClient {

    // TODO - this is never used?
    private static final int MAX_REDIRECT_COUNT = 1;
    private static final int EXTRA_TIMEOUT_FOR_CLIENT_SIDE = (int) TimeUnit.SECONDS.toMillis(30);

    // Make logger available to implementations
    protected static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final HttpClient httpClient;

    public BaseClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    protected Mono<String> postAsync(HttpRequest request, long timeoutMs) {
        return httpClient.send(request, getContextTimeout(timeoutMs))
                .flatMap(response -> Mono.using(
                        () -> response,
                        this::processResponseBodyAsync,
                        HttpResponse::close))
                .onErrorMap(e -> {
                    if (e instanceof DataServiceException) {
                        return e;
                    }
                    return ExceptionUtils.createExceptionOnPost((Exception) e, request.getUrl(), "async");
                });
    }

    public Mono<String> processResponseBodyAsync(HttpResponse response) {
        Mono<String> responseBodyMono = Utils.getResponseBodyAsMono(response);

        return responseBodyMono
                .flatMap(responseBody -> {
                    switch (response.getStatusCode()) {
                        case HttpStatus.OK:
                            return Mono.justOrEmpty(responseBody);
                        case HttpStatus.TOO_MANY_REQS:
                            return Mono.error(new ThrottleException(response.getRequest().getUrl().toString()));
                        default:
                            return Mono.error(createExceptionFromResponse(
                                    response.getRequest().getUrl().toString(), response, null, responseBody));
                    }
                });
    }

    protected Mono<InputStream> postToStreamingOutputAsync(HttpRequest request, long timeoutMs,
            int currentRedirectCounter, int maxRedirectCount) {
        ResponseState state = new ResponseState();
        return httpClient.send(request, getContextTimeout(timeoutMs))
                .flatMap(httpResponse -> processHttpResponse(httpResponse, state, request, timeoutMs, currentRedirectCounter, maxRedirectCount))
                .onErrorMap(IOException.class, e -> new DataServiceException(request.getUrl().toString(),
                        "postToStreamingOutput failed to get or decompress response stream", e, false))
                .onErrorMap(UncheckedIOException.class, e -> ExceptionUtils.createExceptionOnPost(e, request.getUrl(), "streaming async"))
                .onErrorMap(Exception.class,
                        e -> createExceptionFromResponse(request.getUrl().toString(), state.getHttpResponse(), e, state.getErrorFromResponse()))
                .doFinally(ignored -> closeResourcesIfNeeded(state.isReturnInputStream(), state.getHttpResponse()));
    }

    private Mono<InputStream> processHttpResponse(HttpResponse httpResponse, ResponseState state, HttpRequest request,
            long timeoutMs, int currentRedirectCounter, int maxRedirectCount) {
        try {
            state.setHttpResponse(httpResponse);
            int responseStatusCode = httpResponse.getStatusCode();
            if (responseStatusCode == HttpStatus.OK) {
                return handleSuccessfulResponse(httpResponse, state, request);
            }

            return handleErrorResponse(httpResponse, state, request, timeoutMs, currentRedirectCounter, maxRedirectCount);
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    private static Mono<InputStream> handleSuccessfulResponse(HttpResponse httpResponse, ResponseState state, HttpRequest request) {
        state.setReturnInputStream(true);
        return httpResponse.getBodyAsInputStream()
                .flatMap(inputStream -> Mono.fromCallable(() -> {
                    try {
                        return new EofSensorInputStream(new CloseParentResourcesStream(httpResponse, inputStream), null);
                    } catch (IOException e) {
                        throw new DataServiceException(request.getUrl().toString(),
                                "Failed to create EofSensorInputStream", e, false);
                    }
                }));
    }

    private Mono<InputStream> handleErrorResponse(HttpResponse httpResponse, ResponseState state, HttpRequest request,
            long timeoutMs, int currentRedirectCounter, int maxRedirectCount) {
        return httpResponse.getBodyAsByteArray()
                .flatMap(bytes -> {
                    try {
                        String content = Utils.getContentAsString(httpResponse, bytes);
                        state.setErrorFromResponse(content);
                        if (content.isEmpty() || content.equals("{}")) {
                            return Mono.error(new DataServiceException(request.getUrl().toString(),
                                    "postToStreamingOutputAsync failed to get or decompress response body.",
                                    true));
                        }

                        // Ideal to close here (as opposed to finally) so that if any data can't be flushed, the exception will be properly thrown and handled
                        httpResponse.close();

                        if (shouldPostToOriginalUrlDueToRedirect(httpResponse.getStatusCode(), currentRedirectCounter, maxRedirectCount)) {
                            Optional<HttpHeader> redirectLocation = Optional.ofNullable(httpResponse.getHeaders().get(HttpHeaderName.LOCATION));
                            if (redirectLocation.isPresent() && !redirectLocation.get().getValue().equals(request.getUrl().toString())) {
                                HttpRequest redirectRequest = HttpRequestBuilder
                                        .fromExistingRequest(request)
                                        .withURL(redirectLocation.get().getValue())
                                        .build();
                                return postToStreamingOutputAsync(redirectRequest, timeoutMs, currentRedirectCounter + 1, maxRedirectCount);
                            }
                        }
                    } catch (Exception e) {
                        return Mono.error(e);
                    }

                    return Mono.error(createExceptionFromResponse(request.getUrl().toString(), httpResponse, null, state.getErrorFromResponse()));
                });
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

        return RequestUtils.contextWithTimeout(Duration.ofMillis(requestTimeout));
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
