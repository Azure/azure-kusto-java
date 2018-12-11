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
    private HashMap<String, Object> properties;
    private static final String OPTIONS_KEY = "Options";
    private static final String OptionServerTimeout  = "servertimeout";

    public ClientRequestProperties() {
        properties = new HashMap<>();
        properties.put(OPTIONS_KEY, new HashMap<String, Object>());
    }

    public void setOption(String name, Object value) {
        ((HashMap<String, Object>)properties.get(OPTIONS_KEY)).put(name, value);
    }

    public Object getOption(String name) {
        return ((HashMap<String, Object>)properties.get(OPTIONS_KEY)).get(name);
    }

    public void removeOption(String name) {
        ((HashMap<String, Object>)properties.get(OPTIONS_KEY)).remove(name);
    }

    public void clearOptions() {
        ((HashMap<String, Object>)properties.get(OPTIONS_KEY)).clear();
    }

    public void setTimeout(Long timeoutInMs) {
        ((HashMap<String, Object>)properties.get(OPTIONS_KEY)).put(OptionServerTimeout, timeoutInMs);
    }

    public Long getTimeout() {
        return (Long)getOption(OptionServerTimeout);
    }

    public JSONObject toJson() {
        return new JSONObject(properties);
    }

    public String toString() {
        return toJson().toString();
    }
}
