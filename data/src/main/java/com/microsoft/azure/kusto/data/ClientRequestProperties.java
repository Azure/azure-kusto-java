// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;

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
    private static final String OPTION_SERVER_TIMEOUT = "servertimeout";
    private HashMap<String, Object> parameters;
    private HashMap<String, Object> options;

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
        }
        if (timeoutObj instanceof String) {
            timeout = Long.valueOf((String) timeoutObj);
        } else if (timeoutObj instanceof Integer) {
            timeout = Long.valueOf((Integer) timeoutObj);
        }
        return timeout;
    }

    public void setTimeoutInMilliSec(Long timeoutInMs) {
        options.put(OPTION_SERVER_TIMEOUT, timeoutInMs);
    }

    JSONObject toJson() {
        try {
            JSONObject optionsAsJSON = new JSONObject(this.options);
            Long timeoutInMilliSec = getTimeoutInMilliSec();
            if(timeoutInMilliSec != null) {
                optionsAsJSON.put(OPTION_SERVER_TIMEOUT, msToTimespan(timeoutInMilliSec));
            }
            JSONObject json = new JSONObject();
            json.put(OPTIONS_KEY, optionsAsJSON);
            json.put(PARAMETERS_KEY, new JSONObject(this.parameters));
            return json;
        } catch (JSONException e) {
            return null;
        }
    }

    private static String msToTimespan(Long duration) {
        long milliseconds = duration % 1000;
        long seconds = (duration / 1000) % 60;
        long minutes = (duration / (1000 * 60)) % 60;
        long hours = (duration / (1000 * 60 * 60)) % 24;

        String hoursString = (hours < 10) ? "0" + hours : String.valueOf(hours);
        String minutesString = (minutes < 10) ? "0" + minutes : String.valueOf(minutes);
        String secondsString = (seconds < 10) ? "0" + seconds : String.valueOf(seconds);

        return hoursString + ":" + minutesString + ":" + secondsString + "." + String.valueOf(milliseconds);
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

    Iterator<HashMap.Entry<String, Object>> getOptions() {
        return options.entrySet().iterator();
    }
}
