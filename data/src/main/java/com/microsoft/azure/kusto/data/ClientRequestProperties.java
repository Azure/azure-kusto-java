package com.microsoft.azure.kusto.data;

import org.json.JSONObject;

import java.util.HashMap;

/*
 * Kusto supports attaching various properties to client requests (such as queries and control commands).
 * Such properties may be used to provide additional information to Kusto (for example, for the purpose of correlating client/service interaction),
 * may affect what limits and policies get applied to the request, and much more.
 * For a complete list of available client request properties
 * check out https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
 */
public class ClientRequestProperties {
    private static final String OPTIONS_KEY = "Options";
    private static final String OPTION_SERVER_TIMEOUT = "servertimeout";
    private HashMap<String, Object> properties;
    private HashMap<String, Object> options;

    public ClientRequestProperties() {
        properties = new HashMap<>();
        options = new HashMap<>();
        properties.put(OPTIONS_KEY, options);
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

    public Long getTimeoutInMilliSec() {
        return (Long) getOption(OPTION_SERVER_TIMEOUT);
    }

    public void setTimeoutInMilliSec(Long timeoutInMs) {
        options.put(OPTION_SERVER_TIMEOUT, timeoutInMs);
    }

    JSONObject toJson() {
        return new JSONObject(properties);
    }

    public String toString() {
        return toJson().toString();
    }
}
