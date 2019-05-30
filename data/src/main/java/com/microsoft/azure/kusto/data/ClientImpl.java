package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

public class ClientImpl implements Client {

    private static final String ADMIN_COMMANDS_PREFIX = ".";
    private static final String API_VERSION = "v1";
    private static final String DEFAULT_DATABASE_NAME = "NetDefaultDb";
    private static final Long COMMAND_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(10) + TimeUnit.SECONDS.toMillis(30);
    private static final Long QUERY_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(4) + TimeUnit.SECONDS.toMillis(30);
    private static final Long STREAMING_INGEST_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(10);

    private AadAuthenticationHelper aadAuthenticationHelper;
    private String clusterUrl;
    private String clientVersionForTracing;
    private String applicationNameForTracing;

    public ClientImpl(ConnectionStringBuilder csb) throws URISyntaxException {
        clusterUrl = csb.getClusterUrl();
        aadAuthenticationHelper = new AadAuthenticationHelper(csb);
        clientVersionForTracing = "Kusto.Java.Client";
        String version = Utils.GetPackageVersion();
        if (StringUtils.isNotBlank(version)) {
            clientVersionForTracing += ":" + version;
        }

        if (StringUtils.isNotBlank(csb.getClientVersionForTracing())) {
            clientVersionForTracing += "[" + csb.getClientVersionForTracing() + "]";
        }
        applicationNameForTracing = csb.getApplicationNameForTracing();
    }

    @Override
    public Results execute(String command) throws DataServiceException, DataClientException {
        return execute(DEFAULT_DATABASE_NAME, command);
    }

    @Override
    public Results execute(String database, String command) throws DataServiceException, DataClientException {
        return execute(database, command, null);
    }

    @Override
    public Results execute(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException {
        // Argument validation:
        if (StringUtils.isAnyEmpty(database, command)) {
            throw new IllegalArgumentException("database or command are empty");
        }

        Long timeoutMs = null;

        if (properties != null) {
            timeoutMs = properties.getTimeoutInMilliSec();
        }

        String clusterEndpoint;
        if (command.startsWith(ADMIN_COMMANDS_PREFIX)) {
            clusterEndpoint = String.format("%s/%s/rest/mgmt", clusterUrl, API_VERSION);
            if (timeoutMs == null) {
                timeoutMs = COMMAND_TIMEOUT_IN_MILLISECS;
            }
        } else {
            clusterEndpoint = String.format("%s/%s/rest/query", clusterUrl, API_VERSION);
            if (timeoutMs == null) {
                timeoutMs = QUERY_TIMEOUT_IN_MILLISECS;
            }
        }

        String aadAccessToken = aadAuthenticationHelper.acquireAccessToken();
        String jsonString;
        try {
            JSONObject json = new JSONObject()
                    .put("db", database)
                    .put("csl", command);

            if (properties != null) {
                json.put("properties", properties.toString());
            }

            jsonString = json.toString();
        } catch (JSONException e) {
            throw new DataClientException(clusterEndpoint, String.format(clusterEndpoint, "Error in executing command: %s, in database: %s", command, database), e);
        }

        return Utils.post(clusterEndpoint, aadAccessToken, jsonString, null, timeoutMs.intValue(), clientVersionForTracing, applicationNameForTracing);
    }

    @Override
    public Results executeStreamingIngest(String database, String table, InputStream stream, String streamFormat, ClientRequestProperties properties, String mappingName) throws DataServiceException, DataClientException {

        if (StringUtils.isAnyEmpty(database,table,streamFormat))
        {
            throw new IllegalArgumentException("database, table or streamFormat are empty");
        }

        String clusterEndpoint = String.format("%s/%s/rest/ingest/%s/%s?streamFormat=%s",clusterUrl,API_VERSION,database,table,streamFormat);

        if (!StringUtils.isEmpty(mappingName))
        {
            clusterEndpoint.concat(String.format("&mappingName=%s",mappingName));
        }

        String aadAccessToken = aadAuthenticationHelper.acquireAccessToken();

        return Utils.post(clusterEndpoint, aadAccessToken, null, stream, STREAMING_INGEST_TIMEOUT_IN_MILLISECS.intValue(), clientVersionForTracing, applicationNameForTracing);
    }
}