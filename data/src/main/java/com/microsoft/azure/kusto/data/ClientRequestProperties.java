// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.kusto.data.exceptions.KustoParseException;
import com.microsoft.azure.kusto.data.format.CslBoolFormat;
import com.microsoft.azure.kusto.data.format.CslDateTimeFormat;
import com.microsoft.azure.kusto.data.format.CslIntFormat;
import com.microsoft.azure.kusto.data.format.CslLongFormat;
import com.microsoft.azure.kusto.data.format.CslRealFormat;
import com.microsoft.azure.kusto.data.format.CslTimespanFormat;
import com.microsoft.azure.kusto.data.format.CslUuidFormat;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * Kusto supports attaching various properties to client requests (such as queries and control commands).
 * Such properties may be used to provide additional information to Kusto (for example, for the purpose of correlating client/service interaction),
 * may affect what limits and policies get applied to the request, and much more.
 * For a complete list of available client request properties
 * check out https://docs.microsoft.com/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
 */
public class ClientRequestProperties implements Serializable, TraceableAttributes {
    public static final String OPTION_SERVER_TIMEOUT = "servertimeout";

    // If set and positive, indicates the maximum number of HTTP redirects that the client will process. [Integer]
    public static final String OPTION_CLIENT_MAX_REDIRECT_COUNT = "client_max_redirect_count";
    /*
     * Matches valid Kusto Timespans: Optionally negative, optional number of days followed by a period, optionally up to 24 as hours followed by a colon,
     * followed by up to 59 minutes (required), followed by up to 59 seconds (required), followed by optional subseconds prepended by a period. For example:
     * 3.20:40:22 representing 3 days, 30 hours, 40 minutes and 22 seconds or -33:21.4551 representing -33 minutes, 21 seconds, and 4551 1/10000ths of a second
     */
    public static final Pattern KUSTO_TIMESPAN_REGEX = Pattern.compile("(-?)(?:(\\d+)(\\.))?(?:([0-2]?\\d)(:))?([0-5]?\\d)(:)([0-5]?\\d)(?:(\\.)(\\d+))?",
            Pattern.CASE_INSENSITIVE);
    private static final String OPTIONS_KEY = "Options";
    private static final String PARAMETERS_KEY = "Parameters";
    private final Map<String, Object> parameters;
    private final Map<String, Object> options;
    static final long MIN_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);
    static final long MAX_TIMEOUT_MS = TimeUnit.HOURS.toMillis(1);
    private String clientRequestId;
    private String application;
    private String user;

    public ClientRequestProperties() {
        parameters = new HashMap<>();
        options = new HashMap<>();
    }

    public void setOption(String name, Object value) {
        options.put(name, value);
    }

    public Object getOption(String name) {
        return options.get(name);
    }

    public int getRedirectCount() {
        Object optionClientMaxRedirectOption = getOption(OPTION_CLIENT_MAX_REDIRECT_COUNT);
        int optionClientMaxRedirectCount = 0;
        if (optionClientMaxRedirectOption instanceof Integer) {
            optionClientMaxRedirectCount = (int) optionClientMaxRedirectOption;
        } else if (optionClientMaxRedirectOption instanceof String) {
            try {
                optionClientMaxRedirectCount = Integer.parseInt((String) optionClientMaxRedirectOption);
            } catch (NumberFormatException ignore) {
            }
        }

        return Math.max(optionClientMaxRedirectCount, 0);
    }

    public void removeOption(String name) {
        options.remove(name);
    }

    public void clearOptions() {
        options.clear();
    }

    public void setParameter(String name, Object value) {
        parameters.put(name, value);
    }

    public void setParameter(String name, String value) {
        Ensure.stringIsNotBlank(name, "name");
        Ensure.argIsNotNull(value, "value");

        parameters.put(name, value);
    }

    public void setParameter(String name, Date value) {
        Ensure.stringIsNotBlank(name, "name");
        Ensure.argIsNotNull(value, "value");

        parameters.put(name, new CslDateTimeFormat(value).toString());
    }

    public void setParameter(String name, LocalDateTime value) {
        Ensure.stringIsNotBlank(name, "name");
        Ensure.argIsNotNull(value, "value");

        parameters.put(name, new CslDateTimeFormat(value).toString());
    }

    public void setParameter(String name, Duration value) {
        Ensure.stringIsNotBlank(name, "name");
        Ensure.argIsNotNull(value, "value");

        parameters.put(name, new CslTimespanFormat(value).toString());
    }

    public void setParameter(String name, boolean value) {
        Ensure.stringIsNotBlank(name, "name");

        parameters.put(name, new CslBoolFormat(value).toString());
    }

    public void setParameter(String name, int value) {
        Ensure.stringIsNotBlank(name, "name");

        parameters.put(name, new CslIntFormat(value).toString());
    }

    public void setParameter(String name, long value) {
        Ensure.stringIsNotBlank(name, "name");

        parameters.put(name, new CslLongFormat(value).toString());
    }

    public void setParameter(String name, double value) {
        Ensure.stringIsNotBlank(name, "name");

        parameters.put(name, new CslRealFormat(value).toString());
    }

    public void setParameter(String name, UUID value) {
        Ensure.stringIsNotBlank(name, "name");
        Ensure.argIsNotNull(value, "value");

        parameters.put(name, new CslUuidFormat(value).toString());
    }

    public Object getParameter(String name) {
        return parameters.get(name);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public void clearParameters() {
        parameters.clear();
    }

    /**
     * Gets the amount of time a query may execute on the service before it times out. Value must be between 1 minute and 1 hour,
     * and so if the value had been set below the minimum or above the maximum, the value returned will be adjusted accordingly.
     */
    public Long getTimeoutInMilliSec() throws KustoParseException {
        return getTimeoutInMilliSec(getOption(OPTION_SERVER_TIMEOUT));
    }

    private static Long getTimeoutInMilliSec(Object timeoutObj) throws KustoParseException {
        if (timeoutObj == null) {
            return null;
        }

        Long timeout = null;
        if (timeoutObj instanceof Long) {
            timeout = (Long) timeoutObj;
        } else if (timeoutObj instanceof String) {
            timeout = parseTimeoutFromTimespanString((String) timeoutObj);
        } else if (timeoutObj instanceof Integer) {
            timeout = Long.valueOf((Integer) timeoutObj);
        }

        return adjustTimeoutToServiceLimits(timeout);
    }

    private static long parseTimeoutFromTimespanString(String str) throws KustoParseException {
        Matcher matcher = KUSTO_TIMESPAN_REGEX.matcher(str);
        if (!matcher.matches()) {
            throw new KustoParseException(String.format("Failed to parse timeout string as a timespan. Value: '%s'", str));
        }

        if ("-".equals(matcher.group(1))) {
            throw new IllegalArgumentException(String.format("Negative timeouts are invalid. Value: '%s'", str));
        }

        String days = matcher.group(2);
        if (days != null && !days.equals("0") && !days.equals("00")) {
            return MAX_TIMEOUT_MS;
        }

        String timespanWithoutDays = "";
        for (int i = 4; i <= 10; i++) {
            if (matcher.group(i) != null) {
                timespanWithoutDays += matcher.group(i);
            }
        }

        return TimeUnit.NANOSECONDS.toMillis(LocalTime.parse(timespanWithoutDays).toNanoOfDay());
    }

    /**
     * Sets the amount of time a query may execute on the service before it times out.
     * @param timeoutInMs number of milliseconds before timeout.
     *                    Value must be between 1 minute and 1 hour, and so value below the minimum or above the maximum will be adjusted accordingly.
     */
    public void setTimeoutInMilliSec(Long timeoutInMs) {
        options.put(OPTION_SERVER_TIMEOUT, adjustTimeoutToServiceLimits(timeoutInMs));
    }

    private static Long adjustTimeoutToServiceLimits(Long timeoutInMs) {
        if (timeoutInMs != null) {
            if (timeoutInMs < MIN_TIMEOUT_MS) {
                return MIN_TIMEOUT_MS;
            } else if (timeoutInMs > MAX_TIMEOUT_MS) {
                return MAX_TIMEOUT_MS;
            }
        }

        return timeoutInMs;
    }

    JsonNode toJson() {
        ObjectNode optionsAsJSON = Utils.getObjectMapper().valueToTree(this.options);
        Object timeoutObj = getOption(OPTION_SERVER_TIMEOUT);
        if (timeoutObj != null) {
            optionsAsJSON.put(OPTION_SERVER_TIMEOUT, getTimeoutAsCslTimespan(timeoutObj));
        }

        ObjectNode json = Utils.getObjectMapper().createObjectNode();
        json.set(OPTIONS_KEY, optionsAsJSON);
        json.set(PARAMETERS_KEY, Utils.getObjectMapper().valueToTree(this.parameters));
        return json;
    }

    public String toString() {
        return toJson().toString();
    }

    public static ClientRequestProperties fromString(String json) throws JsonProcessingException {
        if (StringUtils.isNotBlank(json)) {
            ClientRequestProperties crp = new ClientRequestProperties();
            JsonNode jsonObj = Utils.getObjectMapper().readTree(json);
            Iterator<String> it = jsonObj.fieldNames();
            while (it.hasNext()) {
                String propertyName = it.next();
                if (propertyName.equals(OPTIONS_KEY)) {
                    JsonNode optionsJson = jsonObj.get(propertyName);
                    Iterator<String> optionsIt = optionsJson.fieldNames();
                    while (optionsIt.hasNext()) {
                        String optionName = optionsIt.next();
                        crp.setOption(optionName, optionsJson.get(optionName).asText());
                    }
                } else if (propertyName.equals(PARAMETERS_KEY)) {
                    JsonNode parameters = jsonObj.get(propertyName);
                    Iterator<String> parametersIt = parameters.fieldNames();
                    while (parametersIt.hasNext()) {
                        String parameterName = parametersIt.next();
                        crp.setParameter(parameterName, parameters.get(parameterName).asText());
                    }
                }
            }
            return crp;
        }

        return null;
    }

    public String getClientRequestId() {
        return clientRequestId;
    }

    public void setClientRequestId(String clientRequestId) {
        this.clientRequestId = clientRequestId;
    }

    /**
     * Gets the application name for tracing purposes.
     * Overrides the application name set in the connection string.
     * @return The application name.
     */
    public String getApplication() {
        return application;
    }

    /**
     * Sets the application name for tracing purposes.
     * Overrides the application name set in the connection string.
     * @param application The application name.
     */
    public void setApplication(String application) {
        this.application = application;
    }

    /**
     * Gets the application version for tracing purposes.
     * Overrides the application version set in the connection string.
     * @return The application version.
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets the application username for tracing purposes.
     * Overrides the application username set in the connection string.
     * @param user The application username.
     */
    public void setUser(String user) {
        this.user = user;
    }

    Iterator<Map.Entry<String, Object>> getOptions() {
        return options.entrySet().iterator();
    }

    public Map<String, String> getTracingAttributes() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("clientRequestId", getClientRequestId());
        return attributes;
    }

    /**
     * Gets the amount of time a query may execute on the service before it times out, formatted as a KQL timespan.
     * @param timeoutObj amount of time before timeout, which may be a Long, String or Integer.
     *                    Value must be between 1 minute and 1 hour, and so value below the minimum or above the maximum will be adjusted accordingly.
     */
    String getTimeoutAsCslTimespan(Object timeoutObj) {
        Long timeoutInMilliSec = getTimeoutInMilliSec(timeoutObj);

        if (timeoutInMilliSec == null) {
            return null;
        }

        Duration duration = Duration.ofMillis(timeoutInMilliSec);
        return Utils.formatDurationAsTimespan(duration);
    }

    /**
     * Gets the amount of time a query may execute on the service before it times out, formatted as a KQL timespan.
     * Value must be between 1 minute and 1 hour, and so if the value had been set below the minimum or above the maximum, the value returned will be adjusted accordingly.
     */
    String getTimeoutAsCslTimespan() {
        return getTimeoutAsCslTimespan(getOption(OPTION_SERVER_TIMEOUT));
    }
}
