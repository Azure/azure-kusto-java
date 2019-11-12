package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.KustoServiceError;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.SQLException;
import java.util.*;

public class KustoResponseResultSet implements Iterator<KustoResultTable> {
    private final static Map<String, WellKnownDataSet> tablesKindsMap = new HashMap<String, WellKnownDataSet>(){{
        put("QueryResult",WellKnownDataSet.PrimaryResult);
        put("QueryProperties",WellKnownDataSet.QueryProperties);
        put("QueryStatus",WellKnownDataSet.QueryCompletionInformation);
    }};

    private ArrayList<KustoResultTable> resultTables = new ArrayList<>();
    private final Iterator<KustoResultTable> it;

    public KustoResponseResultSet(String response, boolean isV2) throws JSONException, KustoServiceError, SQLException {
        if (isV2){
            createFromV2Response(response);
        } else {
            createFromV1Response(response);
        }
        it = resultTables.iterator();
    }

    public ArrayList<KustoResultTable> getResultTables() {
        return resultTables;
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public KustoResultTable next() {
        return it.next();
    }

    public KustoResultTable primaryResults(){
        if (resultTables.size() == 1) {
            return resultTables.get(0);
        }

        return resultTables.stream().filter(t -> t.getTableKind().equals(WellKnownDataSet.PrimaryResult)).findFirst().get();
    }

    private void createFromV1Response(String response) throws JSONException, KustoServiceError, SQLException {
        JSONObject jsonObject = new JSONObject(response);
        JSONArray jsonArray = jsonObject.getJSONArray("Tables");
        for (int i = 0; i < jsonArray.length(); i++){
            JSONObject table = jsonArray.getJSONObject(i);
            resultTables.add(new KustoResultTable(table));
        }

        if(resultTables.size() <= 2){
            resultTables.get(0).setTableKind(WellKnownDataSet.PrimaryResult);
            resultTables.get(0).setTableId(0);

            if (resultTables.size() == 2){
                resultTables.get(1).setTableKind(WellKnownDataSet.QueryProperties);
                resultTables.get(1).setTableId(1);
            }
        } else {
            KustoResultTable toc = resultTables.get(resultTables.size() - 1);
            toc.setTableKind(WellKnownDataSet.TableOfContents);
            toc.setTableId(resultTables.size() - 1);
            for (int i = 0; i < resultTables.size() - 1; i++) {
                JSONObject object = (JSONObject) toc.getObject(i);
                resultTables.get(i).setTableName(object.getString("Name"));
                resultTables.get(i).setTableId(toc.getInt("Id"));
                resultTables.get(i).setTableKind(tablesKindsMap.get(toc.getString("Kind")));
            }
        }
    }

    private void createFromV2Response(String response) throws JSONException, KustoServiceError {
        JSONArray jsonArray = new JSONArray(response);
        for (int i = 0; i < jsonArray.length(); i++){
            JSONObject table = jsonArray.getJSONObject(i);
            if (table.optString("FrameType").equals("DataTable")){
                resultTables.add(new KustoResultTable(table));
            }
        }
    }
}
