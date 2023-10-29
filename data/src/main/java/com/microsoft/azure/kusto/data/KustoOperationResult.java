// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.microsoft.azure.kusto.data.exceptions.JsonPropertyMissingException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.SupplierOneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;

public class KustoOperationResult implements Iterator<KustoResultSetTable> {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
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

    private final ObjectMapper objectMapper = Utils.getObjectMapper();

    public KustoOperationResult(String response, String version) throws KustoServiceQueryError {
        MonitoredActivity.invoke((SupplierOneException<Void, KustoServiceQueryError>) () -> {
            kustoOperationResultImpl(response, version);
            return null;
        }, "KustoOperationResult.createFromResponse");
        it = resultTables.iterator();
    }

    private void kustoOperationResultImpl(String response, String version) throws KustoServiceQueryError {
        if (version.contains("v2")) {
            createFromV2Response(response);
        } else {
            createFromV1Response(response);
        }
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

    private void createFromV1Response(String response) throws KustoServiceQueryError {
        try {
            JsonNode jsonObject = objectMapper.readTree(response);
            if (jsonObject.has(TABLES_LIST_PROPERTY_NAME) && jsonObject.get(TABLES_LIST_PROPERTY_NAME).isArray()) {
                ArrayNode jsonArray = (ArrayNode) jsonObject.get(TABLES_LIST_PROPERTY_NAME);
                for (int i = 0; i < jsonArray.size(); i++) {
                    JsonNode table = jsonArray.get(i);
                    resultTables.add(new KustoResultSetTable(table));
                }
            } else {
                throw new JsonPropertyMissingException("Tables Property missing from V1 response json");
            }
        } catch (JsonProcessingException | JsonPropertyMissingException e) {
            log.error("Json processing error occurred while parsing string to json with exception", e);
            throw new KustoServiceQueryError("Json processing error occurred while parsing string to json with exception " + e.getMessage());
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
        ArrayNode jsonArray;
        try {
            JsonNode jsonNode = objectMapper.readTree(response);
            if (jsonNode.isArray()) {
                jsonArray = (ArrayNode) jsonNode;
                for (JsonNode node : jsonArray) {
                    if (node.has(FRAME_TYPE_PROPERTY_NAME) && node.get(FRAME_TYPE_PROPERTY_NAME).asText().equals(DATA_TABLE_FRAME_TYPE_PROPERTY_NAME)) {
                        resultTables.add(new KustoResultSetTable(node));
                    }
                }
            } else {
                throw new JsonPropertyMissingException("There is no array in the response which can be parsed");
            }
        } catch (JsonProcessingException | JsonPropertyMissingException jsonException) {
            log.error("Json processing error occurred while parsing string to json with exception", jsonException);
            throw new KustoServiceQueryError(
                    "Json processing error occurred while parsing string to json with exception " + jsonException.getMessage());
        } catch (NullPointerException nullPointerException) {
            log.error("Null pointer exception thrown due to invalid v2 response", nullPointerException);
            throw new KustoServiceQueryError("Null pointer exception thrown due to invalid v2 response " + nullPointerException.getMessage());
        }
    }
}
