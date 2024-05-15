package com.microsoft.azure.kusto.data;

import com.azure.core.http.*;
import com.azure.core.util.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.azure.kusto.data.exceptions.*;
import com.microsoft.azure.kusto.data.http.HttpRequestBuilder;
import com.microsoft.azure.kusto.data.http.HttpStatus;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.function.BiConsumer;

public abstract class BaseClient implements Client, StreamingClient {

    private static final int MAX_REDIRECT_COUNT = 1;

    // Make logger available to implementations
    protected static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final HttpClient httpClient;

    public BaseClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    protected String post(HttpRequest request) throws DataServiceException {
        // Execute and get the response
        try (HttpResponse response = httpClient.sendSync(request, Context.NONE)) {
            return processResponseBody(response);
        }
    }

    protected Mono<String> postAsync(HttpRequest request) {
        // Execute and get the response
        return httpClient.send(request)
                .handle(processResponseBodyAsync);
    }

    // TODO: Asaf and Ohad, thoughts?
    private final BiConsumer<HttpResponse, SynchronousSink<String>> processResponseBodyAsync = (response, sink) -> {
        // To the best of my knowledge, reactive pipelines cannot throw checked exceptions, as they must always complete normally.

        // So, in the case of reactive streams we do not want to throw exceptions,
        // but instead insert them into the pipeline as an error state.

        // This leaves two simple options:
        // 1. Slightly abandon DRY and repeat some lines of code when doing synchronous transformations.
        // 2. Use exceptions from synchronous methods as flow control and take a slight performance hit.

        // In short, instead of making the sync methods blocking wrappers of the async methods, I recommended wrapping
        // synchronous transformations like those done below, in order to fit them into a reactive pipeline appropriately.

        // Commented code below is an alternative approach that doesn't duplicate code but instead uses exceptions as flow control.
        // It is slightly slower on error due to a performance hit from catching exceptions
        //try {
        //    String body = processResponseBody(response);
        //    if (body != null) {
        //        sink.next(body);
        //    }
        //    sink.complete();
        //} catch (Exception e) {
        //    sink.error(e);
        //}

        // And here's the main idea, it is slightly redundant/less DRY but uses sink and completes normally without using exceptions as flow control, which is generally not good.
        String responseBody = Utils.isGzipResponse(response) ? Utils.gzipedInputToString(response.getBodyAsBinaryData().toStream())
                : response.getBodyAsBinaryData().toString();

        if (responseBody != null) {
            switch (response.getStatusCode()) {
                case HttpStatus.OK:
                    sink.next(responseBody);
                case HttpStatus.TOO_MANY_REQS:
                    sink.error(new ThrottleException(response.getRequest().getUrl().toString()));
                default:
                    sink.error(createExceptionFromResponse(response.getRequest().getUrl().toString(), response, null, responseBody));
            }
        }
        // If null, complete void
        sink.complete();
    };


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

    protected InputStream postToStreamingOutput(HttpRequest request) throws DataServiceException {
        return postToStreamingOutput(request, 0);
    }

    protected InputStream postToStreamingOutput(HttpRequest request, int redirectCount) throws DataServiceException {

        boolean returnInputStream = false;
        String errorFromResponse = null;

        HttpResponse httpResponse = null;
        try {

            // Todo: Implement async version of this method
            httpResponse = httpClient.sendSync(request, Context.NONE);

            int responseStatusCode = httpResponse.getStatusCode();

            if (responseStatusCode == HttpStatus.OK) {
                returnInputStream = true;
                return Utils.getResponseAsStream(httpResponse);
            }

            errorFromResponse = httpResponse.getBodyAsBinaryData().toString();
            // Ideal to close here (as opposed to finally) so that if any data can't be flushed, the exception will be properly thrown and handled
            httpResponse.close();

            if (shouldPostToOriginalUrlDueToRedirect(redirectCount, responseStatusCode)) {
                Optional<HttpHeader> redirectLocation = Optional.ofNullable(httpResponse.getHeaders().get(HttpHeaderName.LOCATION));
                if (redirectLocation.isPresent() && !redirectLocation.get().getValue().equals(request.getUrl().toString())) {
                    HttpRequest redirectRequest = HttpRequestBuilder
                            .fromExistingRequest(request)
                            .withURL(redirectLocation.get().getValue())
                            .build();
                    return postToStreamingOutput(redirectRequest, redirectCount + 1);
                }
            }
        } catch (IOException ex) {
            throw new DataServiceException(request.getUrl().toString(),
                    "postToStreamingOutput failed to get or decompress response stream", ex, false);
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

    private static void closeResourcesIfNeeded(boolean returnInputStream, HttpResponse httpResponse) {
        // If we close the resources after returning the InputStream to the caller, they won't be able to read from it
        if (!returnInputStream) {
            if (httpResponse != null) {
                httpResponse.close();
            }
        }
    }

    private static boolean shouldPostToOriginalUrlDueToRedirect(int redirectCount, int status) {
        return (status == HttpStatus.FOUND || status == HttpStatus.TEMP_REDIRECT) && redirectCount + 1 <= MAX_REDIRECT_COUNT;
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
