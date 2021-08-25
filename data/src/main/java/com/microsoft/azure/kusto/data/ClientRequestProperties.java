// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.TimespanParseException;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.Duration;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.microsoft.azure.kusto.data.Utils.SECONDS_PER_DAY;

/*
 * Kusto supports attaching various properties to client requests (such as queries and control commands).
 * Such properties may be used to provide additional information to Kusto (for example, for the purpose of correlating client/service interaction),
 * may affect what limits and policies get applied to the request, and much more.
 * For a complete list of available client request properties
 * check out https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
 */
public class ClientRequestProperties {
    private static final String OPTIONS_KEY = "Options";
    private static final String PARAMETERS_KEY = "Parameters";
    public static final String OPTION_SERVER_TIMEOUT = "servertimeout";
    public static final String OPTION_CLIENT_REQUEST_ID = "ClientRequestId";
    private static final long NANOS_TO_MILLIS = 1000000L;
    private final HashMap<String, Object> parameters;
    private final HashMap<String, Object> options;
    private static final Pattern PATTERN =
            Pattern.compile("(\\d+)?\\.?([0-2]?\\d:(?:60|[0-5]\\d):(?:60|[0-5]\\d)(:?\\.\\d+))",
                    Pattern.CASE_INSENSITIVE);
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

    public Object getParameter(String name) {
        return parameters.get(name);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public void clearParameters() {
        parameters.clear();
    }

    public Long getTimeoutInMilliSec() {
        Object timeoutObj = getOption(OPTION_SERVER_TIMEOUT);
        Long timeout = null;
        if (timeoutObj instanceof Long) {
            timeout = (Long) timeoutObj;
        } else if (timeoutObj instanceof String) {
            timeout = getMillisFromTimespanString((String) timeoutObj);
        } else if (timeoutObj instanceof Integer) {
            timeout = Long.valueOf((Integer) timeoutObj);
        }

        return timeout;
    }

    private long getMillisFromTimespanString(String str) throws TimespanParseException {
        Matcher matcher = PATTERN.matcher(str);
        if (!matcher.matches()) {
            throw new TimespanParseException(str);
        }

        long millis = 0;
        String days = matcher.group(1);
        if (days != null) {
            millis += Long.parseLong(days) * SECONDS_PER_DAY * 1000L;
        }

        millis += LocalTime.parse(matcher.group(1)).toNanoOfDay() / 1000L;
        return millis;
    }

    public void setTimeoutInMilliSec(Long timeoutInMs) {
        Duration duration = Duration.ofMillis(timeoutInMs);
        options.put(OPTION_SERVER_TIMEOUT, Utils.formatDurationAsTimespan(duration));
    }

    JSONObject toJson() {
        try {
            JSONObject optionsAsJSON = new JSONObject(this.options);
            Long timeoutInMilliSec = getTimeoutInMilliSec();
            if (timeoutInMilliSec != null) {
                LocalTime localTime = LocalTime.ofNanoOfDay(timeoutInMilliSec * NANOS_TO_MILLIS);
                optionsAsJSON.put(OPTION_SERVER_TIMEOUT, localTime.toString());
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
                    JSONObject options = (JSONObject) jsonObj.get(propertyName);
                    Iterator<String> optionsIt = options.keys();
                    while (optionsIt.hasNext()) {
                        String optionName = optionsIt.next();
                        crp.setOption(optionName, options.get(optionName));
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
