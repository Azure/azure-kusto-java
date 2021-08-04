package com.microsoft.azure.kusto.data;

public enum CommandType {
    ADMIN_COMMAND("%s/" + ClientImpl.MGMT_ENDPOINT_VERSION + "/rest/mgmt", "AdminCommand"),
    QUERY("%s/" + ClientImpl.QUERY_ENDPOINT_VERSION + "/rest/query", "QueryCommand"),
    STREAMING_INGEST("%s/" + ClientImpl.STREAMING_VERSION + "/rest/ingest/%s/%s?streamFormat=%s", "StreamingIngest");

    private final String endpoint;
    private final String activityTypeSuffix;

    CommandType(String endpoint, String activityTypeSuffix) {
        this.endpoint = endpoint;
        this.activityTypeSuffix = activityTypeSuffix;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getActivityTypeSuffix() {
        return activityTypeSuffix;
    }
}
