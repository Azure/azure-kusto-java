package com.microsoft.azure.kusto.data;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;

public class KustoResponseDataSet implements Iterator<ResultSet> {
    private ArrayList<ResultSet> resultSetArrayList;

    public KustoResponseDataSet(String response, boolean isV2) throws JSONException {
        if (isV2){
            createFromV2Response(response);
        } else {
            createFromV1Response(response);
        }
    }

    private void createFromV1Response(String response) throws JSONException {
        JSONObject jsonObject = new JSONObject(response);
        createFromJSONArray(jsonObject.getJSONArray("Tables"));
        if(resultSetArrayList.size() <= 2){

        }
    }

    private void createFromV2Response(String response) throws JSONException {
        createFromJSONArray(new JSONArray(response));
    }

    private void createFromJSONArray(JSONArray jsonArray){
        for (int i = 0; i < jsonArray.length(); i++){
            JSONObject table = jsonArray.getJSONObject(i);
            if (table.optString("FrameType").equals("DataTable")){
                resultSetArrayList.add(new KustoResultTable(table));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public ResultSet next() {
        return null;
    }
}
