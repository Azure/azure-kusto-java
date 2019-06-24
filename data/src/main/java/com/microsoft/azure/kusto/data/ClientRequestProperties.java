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
        return (Long) getOption(OPTION_SERVER_TIMEOUT);
    }

    public void setTimeoutInMilliSec(Long timeoutInMs) {
        options.put(OPTION_SERVER_TIMEOUT, timeoutInMs);
    }

    JSONObject toJson() {
        try {
            JSONObject json = new JSONObject();
            json.put(OPTIONS_KEY, new JSONObject(this.options));
            json.put(PARAMETERS_KEY, new JSONObject(this.parameters));
            return json;
        } catch (JSONException e) {
            return null;
        }
    }

    public String toString() {
        return toJson().toString();
    }

    public static ClientRequestProperties fromString (String json) throws JSONException {
        if (StringUtils.isNotBlank(json)) {
            ClientRequestProperties crp = new ClientRequestProperties();
            JSONObject jsonObj = new JSONObject(json);
            Iterator it = jsonObj.keys();
            while (it.hasNext()) {
                String optionName = (String) it.next();
                crp.setOption(optionName, jsonObj.get(optionName));
            }
            return crp;
        }

        return null;
    }
}
