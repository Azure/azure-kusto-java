package com.microsoft.azure.kusto.data.res;

public class JsonResult {
    private String result;
    private String endpoint;

    public JsonResult(String result, String endpoint) {
        this.result = result;
        this.endpoint = endpoint;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
}
