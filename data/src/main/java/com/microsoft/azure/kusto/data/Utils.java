package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Utils {

    static Results post(String url, String payload, InputStream stream, Integer timeoutMs, HashMap<String, String> headers, boolean leaveOpen) throws DataServiceException, DataClientException {
        HttpClient httpClient;
        if (timeoutMs != null) {
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeoutMs).build();
            httpClient = HttpClientBuilder.create().useSystemProperties().setDefaultRequestConfig(requestConfig).build();
        } else {
            httpClient = HttpClients.createSystem();
        }
        HttpPost httpPost = new HttpPost(url);

        // Request parameters and other properties.
        HttpEntity requestEntity = (stream == null) ? new StringEntity(payload, ContentType.APPLICATION_JSON)
                : new InputStreamEntity(stream);
        httpPost.setEntity(requestEntity);
        httpPost.addHeader("Accept-Encoding", "gzip,deflate");
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            httpPost.addHeader(entry.getKey(), entry.getValue());
        }

        try (InputStream streamToClose = (stream != null && !leaveOpen) ? stream : null) {
            //Execute and get the response.
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();

            if (entity != null) {

                StatusLine statusLine = response.getStatusLine();
                String responseContent = EntityUtils.toString(entity);
                String exceptions = null;
                if (statusLine.getStatusCode() == 200) {

                    JSONObject jsonObject = new JSONObject(responseContent);
                    JSONArray tablesArray = jsonObject.getJSONArray("Tables");
                    JSONObject table0 = tablesArray.getJSONObject(0);
                    JSONArray resultsColumns = table0.getJSONArray("Columns");

                    Map<String, Integer> columnNameToIndex = new HashMap<>();
                    List<String>  columns = new ArrayList<>();
                    Map<String, String> columnNameToType = new HashMap<>();
                    for (int i = 0; i < resultsColumns.length(); i++) {
                        JSONObject column = resultsColumns.getJSONObject(i);
                        String columnName = column.getString("ColumnName");
                        columnNameToIndex.put(columnName, i);
                        columns.add(columnName);
                        columnNameToType.put(columnName, column.getString("DataType"));
                    }

                    JSONArray resultsRows = table0.getJSONArray("Rows");
                    List<List<String>> values = new ArrayList<>();
                    for (int i = 0; i < resultsRows.length(); i++) {
                        Object row = resultsRows.get(i);
                        if (row instanceof JSONObject) {
                            exceptions = ((JSONObject) row).get("Exceptions").toString();
                        }
                        JSONArray rowAsJsonArray = resultsRows.getJSONArray(i);
                        List<String> rowVector = new ArrayList<>();
                        for (int j = 0; j < rowAsJsonArray.length(); ++j) {
                            Object obj = rowAsJsonArray.get(j);
                            if (obj == JSONObject.NULL) {
                                rowVector.add(null);
                            } else {
                                rowVector.add(obj.toString());
                            }
                        }
                        values.add(rowVector);
                    }

                    return new Results(columnNameToIndex,columns, columnNameToType, values, exceptions);
                } else {
                    throw new DataServiceException(url, "Error in post request", new DataWebException(responseContent, response));
                }
            }
        } catch (JSONException | IOException e) {
            throw new DataClientException(url, "Error in post request", e);
        }
        return null;
    }

    static String GetPackageVersion() {
        return Utils.class.getPackage().getImplementationVersion();
    }
}
