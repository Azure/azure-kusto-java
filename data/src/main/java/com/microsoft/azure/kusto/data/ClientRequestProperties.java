// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.format.CslBoolFormat;
import com.microsoft.azure.kusto.data.format.CslDateTimeFormat;
import com.microsoft.azure.kusto.data.format.CslIntFormat;
import com.microsoft.azure.kusto.data.format.CslLongFormat;
import com.microsoft.azure.kusto.data.format.CslRealFormat;
import com.microsoft.azure.kusto.data.format.CslTimespanFormat;
import com.microsoft.azure.kusto.data.format.CslUuidFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.ParseException;
import org.json.JSONException;
import org.json.JSONObject;

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
 * check out https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
 */
public class ClientRequestProperties {
    public static final String OPTION_SERVER_TIMEOUT = "servertimeout";
    public static final String OPTION_CLIENT_REQUEST_ID = "ClientRequestId";
    /*
     * Matches valid Kusto Timespans: Optionally negative, optional number of days followed by a period, optionally up to 24 as hours followed by a colon,
     * followed by up to 59 minutes (required), followed by up to 59 seconds (required), followed by optional subseconds prepended by a period. For example:
     * 3.20:40:22 representing 3 days, 30 hours, 40 minutes and 22 seconds or -33:21.4551 representing -33 minutes, 21 seconds, and 4551 1/10000ths of a second
     */
    public static final Pattern KUSTO_TIMESPAN_REGEX = Pattern.compile("(-?)(?:(\\d+)(\\.))?(?:([0-2]?\\d)(:))?([0-5]?\\d)(:)([0-5]?\\d)(?:(\\.)(\\d+))?", Pattern.CASE_INSENSITIVE);
    private static final String OPTIONS_KEY = "Options";
    private static final String PARAMETERS_KEY = "Parameters";
    private final Map<String, Object> parameters;
    private final Map<String, Object> options;
    static final long MAX_TIMEOUT_MS = TimeUnit.HOURS.toSeconds(1) * 1000;

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

    public Long getTimeoutInMilliSec() throws ParseException {
        Object timeoutObj = getOption(OPTION_SERVER_TIMEOUT);
        Long timeout = null;
        if (timeoutObj instanceof Long) {
            timeout = (Long) timeoutObj;
        } else if (timeoutObj instanceof String) {
            timeout = parseTimeoutFromTimespanString((String) timeoutObj);
        } else if (timeoutObj instanceof Integer) {
            timeout = Long.valueOf((Integer) timeoutObj);
        }

        return timeout;
    }

    private long parseTimeoutFromTimespanString(String str) throws ParseException {
        Matcher matcher = KUSTO_TIMESPAN_REGEX.matcher(str);
        if (!matcher.matches()) {
            throw new ParseException(String.format("Failed to parse timeout string as a timespan. Value: '%s'", str));
        }

        if ("-".equals(matcher.group(1))) {
            throw new IllegalArgumentException(String.format("Negative timeouts are invalid. Value: '%s'", str));
        }
        long millis = 0;
        String days = matcher.group(2);
        if (days != null && !days.equals("0")) {
            return MAX_TIMEOUT_MS;
        }

        String timespanWithoutDays = "";
        for (int i = 4; i <= 10; i++) {
            if (matcher.group(i) != null) {
                timespanWithoutDays += matcher.group(i);
            }
        }
        millis += TimeUnit.NANOSECONDS.toMillis(LocalTime.parse(timespanWithoutDays).toNanoOfDay());
        return millis;
    }

    public void setTimeoutInMilliSec(Long timeoutInMs) {
        options.put(OPTION_SERVER_TIMEOUT, timeoutInMs);
    }

    JSONObject toJson() {
        try {
            JSONObject optionsAsJSON = new JSONObject(this.options);
            Object timeoutObj = getOption(OPTION_SERVER_TIMEOUT);

            if (timeoutObj != null) {
                String timeoutString = "";
                if (timeoutObj instanceof Long) {
                    Duration duration = Duration.ofMillis((Long) timeoutObj);
                    timeoutString = Utils.formatDurationAsTimespan(duration);
                } else if (timeoutObj instanceof String) {
                    timeoutString = (String) timeoutObj;
                } else if (timeoutObj instanceof Integer) {
                    Duration duration = Duration.ofMillis((Integer) timeoutObj);
                    timeoutString = Utils.formatDurationAsTimespan(duration);
                }
                optionsAsJSON.put(OPTION_SERVER_TIMEOUT, timeoutString);
            }
            JSONObject json = new JSONObject();
            json.put(OPTIONS_KEY, optionsAsJSON);
            json.put(PARAMETERS_KEY, new JSONObject(this.parameters));
            return json;
        } catch (JSONException e) {
            return null;
        }
    }

    public String toString() {
        return toJson().toString();
    }

    public static ClientRequestProperties fromString(String json) throws JSONException {
        if (StringUtils.isNotBlank(json)) {
            ClientRequestProperties crp = new ClientRequestProperties();
            JSONObject jsonObj = new JSONObject(json);
            Iterator<String> it = jsonObj.keys();
            while (it.hasNext()) {
                String propertyName = it.next();
                if (propertyName.equals(OPTIONS_KEY)) {
                    JSONObject optionsJson = (JSONObject) jsonObj.get(propertyName);
                    Iterator<String> optionsIt = optionsJson.keys();
                    while (optionsIt.hasNext()) {
                        String optionName = optionsIt.next();
                        crp.setOption(optionName, optionsJson.get(optionName));
                    }
                } else if (propertyName.equals(PARAMETERS_KEY)) {
                    JSONObject parameters = (JSONObject) jsonObj.get(propertyName);
                    Iterator<String> parametersIt = parameters.keys();
                    while (parametersIt.hasNext()) {
                        String parameterName = parametersIt.next();
                        crp.setParameter(parameterName, parameters.get(parameterName));
                    }
                }
            }
            return crp;
        }

        return null;
    }

    public String getClientRequestId() {
        return (String) getOption(OPTION_CLIENT_REQUEST_ID);
    }

    public void setClientRequestId(String clientRequestId) {
        setOption(OPTION_CLIENT_REQUEST_ID, clientRequestId);
    }

    Iterator<HashMap.Entry<String, Object>> getOptions() {
        return options.entrySet().iterator();
    }
}
