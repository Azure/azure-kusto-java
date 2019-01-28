package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

class Utils {

    static Results post(String url, String aadAccessToken, String payload, Integer timeoutMs, String clientVersionForTracing) throws DataServiceException, DataClientException {

        HttpClient httpClient;
        if (timeoutMs != null) {
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeoutMs).build();
            httpClient = HttpClientBuilder.create().useSystemProperties().setDefaultRequestConfig(requestConfig).build();
        } else {
            httpClient = HttpClients.createSystem();
        }

        HttpPost httpPost = new HttpPost(url);

        // Request parameters and other properties.
        StringEntity requestEntity = new StringEntity(
                payload,
                ContentType.APPLICATION_JSON);


        httpPost.setEntity(requestEntity);

        httpPost.addHeader("Authorization", String.format("Bearer %s", aadAccessToken));
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.addHeader("Accept-Encoding", "gzip,deflate");
        httpPost.addHeader("Fed", "True");

        httpPost.addHeader("x-ms-client-version", clientVersionForTracing);
        httpPost.addHeader("x-ms-client-request-id", String.format("KJC.execute;%s", java.util.UUID.randomUUID()));

        try {
            //Execute and get the response.
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();

            if (entity != null) {

                StatusLine statusLine = response.getStatusLine();
                String responseContent = EntityUtils.toString(entity);

                if (statusLine.getStatusCode() == 200) {

                    JSONObject jsonObject = new JSONObject(responseContent);
                    JSONArray tablesArray = jsonObject.getJSONArray("Tables");
                    JSONObject table0 = tablesArray.getJSONObject(0);
                    JSONArray resultsColumns = table0.getJSONArray("Columns");

                    HashMap<String, Integer> columnNameToIndex = new HashMap<>();
                    HashMap<String, String> columnNameToType = new HashMap<>();
                    for (int i = 0; i < resultsColumns.length(); i++) {
                        JSONObject column = resultsColumns.getJSONObject(i);
                        String columnName = column.getString("ColumnName");
                        columnNameToIndex.put(columnName, i);
                        columnNameToType.put(columnName, column.getString("DataType"));
                    }

                    JSONArray resultsRows = table0.getJSONArray("Rows");
                    ArrayList<ArrayList<String>> values = new ArrayList<>();
                    for (int i = 0; i < resultsRows.length(); i++) {
                        JSONArray row = resultsRows.getJSONArray(i);
                        ArrayList<String> rowVector = new ArrayList<>();
                        for (int j = 0; j < row.length(); ++j) {
                            Object obj = row.get(j);
                            if (obj == JSONObject.NULL) {
                                rowVector.add(null);
                            } else {
                                rowVector.add(obj.toString());
                            }
                        }
                        values.add(rowVector);
                    }

                    return new Results(columnNameToIndex, columnNameToType, values);
                } else {
                    throw new DataServiceException(url, "Error in post request", new DataWebException(responseContent, response));
                }
            }
        } catch (JSONException | IOException e) {
            throw new DataClientException(url, "Error in post request", e);
        }
        return null;
    }

    static String GetPackageVersion(){
        return Utils.class.getPackage().getImplementationVersion();
    }
}
