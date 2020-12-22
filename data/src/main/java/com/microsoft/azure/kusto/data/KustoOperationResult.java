// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.KustoServiceError;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

public class KustoOperationResult implements Iterator<KustoResultSetTable> {
    private static final Map<String, WellKnownDataSet> tablesKindsMap = new HashMap<String, WellKnownDataSet>() {{
        put("QueryResult", WellKnownDataSet.PrimaryResult);
        put("QueryProperties", WellKnownDataSet.QueryProperties);
        put("QueryStatus", WellKnownDataSet.QueryCompletionInformation);
    }};
    private static final String tableNamePropertyName = "Name";
    private static final String tableIdPropertyName = "Id";
    private static final String tableKindPropertyName = "Kind";
    private static final String tablesListPropertyName = "Tables";
    private static final String dataTableFrameTypePropertyName = "DataTable";
    private static final String frameTypePropertyName = "FrameType";

    private List<KustoResultSetTable> resultTables = new ArrayList<>();
    private final Iterator<KustoResultSetTable> it;

    public KustoOperationResult(String response, String version) throws JSONException, KustoServiceError {
        if (version.contains("v2")) {
            createFromV2Response(response);
        } else {
            createFromV1Response(response);
        }
        it = resultTables.iterator();
    }

    public List<KustoResultSetTable> getResultTables() {
        return resultTables;
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public KustoResultSetTable next() {
        return it.next();
    }

    public KustoResultSetTable getPrimaryResults() {
        if (resultTables.size() == 1) {
            return resultTables.get(0);
        }

        return resultTables.stream().filter(t -> t.getTableKind().equals(WellKnownDataSet.PrimaryResult)).findFirst().orElse(null);
    }

    private void createFromV1Response(String response) throws KustoServiceError {
        JSONObject jsonObject = new JSONObject(response);
        JSONArray jsonArray = jsonObject.getJSONArray(tablesListPropertyName);
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject table = jsonArray.getJSONObject(i);
            resultTables.add(new KustoResultSetTable(table));
        }

        if (resultTables.size() <= 2) {
            resultTables.get(0).setTableKind(WellKnownDataSet.PrimaryResult);
            resultTables.get(0).setTableId(Integer.toString(0));

            if (resultTables.size() == 2) {
                resultTables.get(1).setTableKind(WellKnownDataSet.QueryProperties);
                resultTables.get(1).setTableId(Integer.toString(1));
            }
        } else {
            KustoResultSetTable toc = resultTables.get(resultTables.size() - 1);
            toc.setTableKind(WellKnownDataSet.TableOfContents);
            toc.setTableId(Integer.toString(resultTables.size() - 1));
            for (int i = 0; i < resultTables.size() - 1; i++) {
                toc.next();
                resultTables.get(i).setTableName(toc.getString(tableNamePropertyName));
                resultTables.get(i).setTableId(toc.getString(tableIdPropertyName));
                resultTables.get(i).setTableKind(tablesKindsMap.get(toc.getString(tableKindPropertyName)));
            }
        }
    }

    private void createFromV2Response(String response) throws JSONException, KustoServiceError {
        JSONArray jsonArray = new JSONArray(response);
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject table = jsonArray.getJSONObject(i);
            if (table.optString(frameTypePropertyName).equals(dataTableFrameTypePropertyName)) {
                resultTables.add(new KustoResultSetTable(table));
            }
        }
    }
}