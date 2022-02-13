// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import java.util.*;
import org.json.JSONArray;
import org.json.JSONObject;

public class KustoOperationResult implements Iterator<KustoResultSetTable> {
    private static final Map<String, WellKnownDataSet> tablesKindsMap = new HashMap<String, WellKnownDataSet>() {
        {
            put("QueryResult", WellKnownDataSet.PrimaryResult);
            put("QueryProperties", WellKnownDataSet.QueryProperties);
            put("QueryStatus", WellKnownDataSet.QueryCompletionInformation);
        }
    };
    private static final String TABLE_NAME_PROPERTY_NAME = "Name";
    private static final String TABLE_ID_PROPERTY_NAME = "Id";
    private static final String TABLE_KIND_PROPERTY_NAME = "Kind";
    private static final String TABLES_LIST_PROPERTY_NAME = "Tables";
    private static final String DATA_TABLE_FRAME_TYPE_PROPERTY_NAME = "DataTable";
    private static final String FRAME_TYPE_PROPERTY_NAME = "FrameType";
    private final List<KustoResultSetTable> resultTables = new ArrayList<>();
    private final Iterator<KustoResultSetTable> it;

    public KustoOperationResult(String response, String version) throws KustoServiceQueryError {
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

        return resultTables.stream()
                .filter(t -> t.getTableKind().equals(WellKnownDataSet.PrimaryResult))
                .findFirst()
                .orElse(null);
    }

    private void createFromV1Response(String response) throws KustoServiceQueryError {
        JSONObject jsonObject = new JSONObject(response);
        JSONArray jsonArray = jsonObject.getJSONArray(TABLES_LIST_PROPERTY_NAME);
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
                resultTables.get(i).setTableName(toc.getString(TABLE_NAME_PROPERTY_NAME));
                resultTables.get(i).setTableId(toc.getString(TABLE_ID_PROPERTY_NAME));
                resultTables.get(i).setTableKind(tablesKindsMap.get(toc.getString(TABLE_KIND_PROPERTY_NAME)));
            }
        }
    }

    private void createFromV2Response(String response) throws KustoServiceQueryError {
        JSONArray jsonArray = new JSONArray(response);
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject table = jsonArray.getJSONObject(i);
            if (table.optString(FRAME_TYPE_PROPERTY_NAME).equals(DATA_TABLE_FRAME_TYPE_PROPERTY_NAME)) {
                resultTables.add(new KustoResultSetTable(table));
            }
        }
    }
}
