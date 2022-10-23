// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

// This class does not keep an open connection with the cluster - the results are evaluated once and can be get by getData()
public class KustoResultSetTable {
    private static final String TABLE_NAME_PROPERTY_NAME = "TableName";
    private static final String TABLE_ID_PROPERTY_NAME = "TableId";
    private static final String TABLE_KIND_PROPERTY_NAME = "TableKind";
    private static final String COLUMNS_PROPERTY_NAME = "Columns";
    private static final String COLUMN_NAME_PROPERTY_NAME = "ColumnName";
    private static final String COLUMN_TYPE_PROPERTY_NAME = "ColumnType";
    private static final String COLUMN_TYPE_SECOND_PROPERTY_NAME = "DataType";
    private static final String ROWS_PROPERTY_NAME = "Rows";
    private static final String EXCEPTIONS_PROPERTY_NAME = "Exceptions";
    private static final String EXCEPTIONS_MESSAGE = "Query execution failed with multiple inner exceptions";
    private static DateTimeFormatter kustoDateTimeFormatter = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME).appendLiteral('Z').toFormatter();

    private final List<List<Object>> rows;
    private String tableName;
    private String tableId;
    private WellKnownDataSet tableKind;
    private final Map<String, KustoResultColumn> columns = new HashMap<>();
    private KustoResultColumn[] columnsAsArray = null;
    private Iterator<List<Object>> rowIterator;
    private List<Object> currentRow = null;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableId() {
        return tableId;
    }

    public WellKnownDataSet getTableKind() {
        return tableKind;
    }

    public KustoResultColumn[] getColumns() {
        return columnsAsArray;
    }

    void setTableId(String tableId) {
        this.tableId = tableId;
    }

    void setTableKind(WellKnownDataSet tableKind) {
        this.tableKind = tableKind;
    }

    protected KustoResultSetTable(JSONObject jsonTable) throws KustoServiceQueryError {
        tableName = jsonTable.optString(TABLE_NAME_PROPERTY_NAME);
        tableId = jsonTable.optString(TABLE_ID_PROPERTY_NAME);
        String tableKindString = jsonTable.optString(TABLE_KIND_PROPERTY_NAME);
        tableKind = StringUtils.isBlank(tableKindString) ? null : WellKnownDataSet.valueOf(tableKindString);
        JSONArray columnsJson = jsonTable.optJSONArray(COLUMNS_PROPERTY_NAME);
        if (columnsJson != null) {
            columnsAsArray = new KustoResultColumn[columnsJson.length()];
            for (int i = 0; i < columnsJson.length(); i++) {
                JSONObject jsonCol = columnsJson.getJSONObject(i);
                String columnType = jsonCol.optString(COLUMN_TYPE_PROPERTY_NAME);
                if (columnType.equals("")) {
                    columnType = jsonCol.optString(COLUMN_TYPE_SECOND_PROPERTY_NAME);
                }
                KustoResultColumn col = new KustoResultColumn(jsonCol.getString(COLUMN_NAME_PROPERTY_NAME), columnType, i);
                columnsAsArray[i] = col;
                columns.put(jsonCol.getString(COLUMN_NAME_PROPERTY_NAME), col);
            }
        }

        JSONArray exceptions;
        JSONArray jsonRows = jsonTable.optJSONArray(ROWS_PROPERTY_NAME);
        if (jsonRows != null) {
            List<List<Object>> values = new ArrayList<>();
            for (int i = 0; i < jsonRows.length(); i++) {
                Object row = jsonRows.get(i);
                if (row instanceof JSONObject) {
                    exceptions = ((JSONObject) row).optJSONArray(EXCEPTIONS_PROPERTY_NAME);
                    if (exceptions != null) {
                        if (exceptions.length() == 1) {
                            String message = exceptions.getString(0);
                            throw new KustoServiceQueryError(message);
                        } else {
                            throw new KustoServiceQueryError(exceptions, false, EXCEPTIONS_MESSAGE);
                        }
                    } else {
                        throw new KustoServiceQueryError(((JSONObject) row).getJSONArray(
                                "OneApiErrors"), true, EXCEPTIONS_MESSAGE);
                    }
                }
                JSONArray rowAsJsonArray = jsonRows.getJSONArray(i);
                List<Object> rowVector = new ArrayList<>();
                for (int j = 0; j < rowAsJsonArray.length(); ++j) {
                    Object obj = rowAsJsonArray.get(j);
                    if (obj == JSONObject.NULL) {
                        rowVector.add(null);
                    } else {
                        rowVector.add(obj);
                    }
                }
                values.add(rowVector);
            }

            rows = values;
        } else {
            rows = new ArrayList<>();
        }

        rowIterator = rows.iterator();
    }

    public List<Object> getCurrentRow() {
        return currentRow;
    }

    public boolean next() {
        boolean hasNext = hasNext();
        if (hasNext) {
            currentRow = rowIterator.next();
        }
        return hasNext;
    }

    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    public List<List<Object>> getData() {
        return rows;
    }

    private Object get(int columnIndex) {
        return currentRow.get(columnIndex);
    }

    private Object get(String colName) {
        return currentRow.get(findColumn(colName));
    }

    public String getString(int columnIndex) {
        return get(columnIndex).toString();
    }

    public boolean getBoolean(int columnIndex) {
        return (boolean) get(columnIndex);
    }

    public Boolean getBooleanObject(int columnIndex) {
        return (Boolean) get(columnIndex);
    }

    public byte getByte(int columnIndex) {
        return (byte) get(columnIndex);
    }

    public short getShort(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj instanceof Integer) {
            return ((Integer) obj).shortValue();
        }
        return (short) get(columnIndex);
    }

    public Short getShortObject(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj == null) {
            return null;
        }
        return getShort(columnIndex);
    }

    public int getInt(int columnIndex) {
        return (int) get(columnIndex);
    }

    public Integer getIntegerObject(int columnIndex) {
        return (Integer) get(columnIndex);
    }

    public long getLong(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj instanceof Integer) {
            return ((Integer) obj).longValue();
        }
        return (long) obj;
    }

    public Long getLongObject(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj instanceof Integer) {
            return ((Integer) obj).longValue();
        }
        return (Long) obj;
    }

    public float getFloat(int columnIndex) {
        return (float) get(columnIndex);
    }

    public Float getFloatObject(int columnIndex) {
        return (Float) get(columnIndex);
    }

    public double getDouble(int columnIndex) {
        return (double) get(columnIndex);
    }

    public Double getDoubleObject(int columnIndex) {
        Object d = get(columnIndex);
        if (d instanceof BigDecimal) {
            return ((BigDecimal) d).doubleValue();
        }
        return (Double) get(columnIndex);
    }

    public byte[] getBytes(int columnIndex) {
        return (byte[]) get(columnIndex);
    }

    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, Calendar.getInstance());
    }

    public Time getTime(int columnIndex) {
        LocalTime time = getLocalTime(columnIndex);
        if (time == null) {
            return null;
        }

        return Time.valueOf(getLocalTime(columnIndex));
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        switch (columnsAsArray[columnIndex].getColumnType()) {
            case "string":
            case "datetime":
                if (get(columnIndex) == null) {
                    return null;
                }
                return Timestamp.valueOf(StringUtils.chop(getString(columnIndex)).replace("T", " "));
            case "long":
            case "int":
                Long l = getLongObject(columnIndex);
                if (l == null) {
                    return null;
                }

                return new Timestamp(l);
        }
        throw new SQLException("Error parsing timestamp - expected string or long columns.");
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLFeatureNotSupportedException {
        if (columnsAsArray[columnIndex].getColumnType().equals("String")) {
            return new ByteArrayInputStream(getString(columnIndex).getBytes());
        }

        throw new SQLFeatureNotSupportedException("getBinaryStream is only available for strings");
    }

    public String getString(String columnName) {
        return get(columnName).toString();
    }

    public boolean getBoolean(String columnName) {
        return (boolean) get(columnName);
    }

    public Boolean getBooleanObject(String columnName) {
        return (Boolean) get(columnName);
    }

    public byte getByte(String columnName) {
        return (byte) get(columnName);
    }

    public short getShort(String columnName) {
        return getShort(findColumn(columnName));
    }

    public Short getShortObject(String columnName) {
        return getShortObject(findColumn(columnName));
    }

    public int getInt(String columnName) {
        return (int) get(columnName);
    }

    public Integer getIntegerObject(String columnName) {
        return getIntegerObject(findColumn(columnName));
    }

    public long getLong(String columnName) {
        return (long) get(columnName);
    }

    public Long getLongObject(String columnName) {
        return getLongObject(findColumn(columnName));
    }

    public float getFloat(String columnName) {
        return (float) get(columnName);
    }

    public Float getFloatObject(String columnName) {
        return getFloatObject(findColumn(columnName));
    }

    public double getDouble(String columnName) {
        return (double) get(columnName);
    }

    public Double getDoubleObject(String columnName) {
        return (Double) get(columnName);
    }

    public byte[] getBytes(String columnName) {
        return (byte[]) get(columnName);
    }

    public Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName));
    }

    public Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName));
    }

    public Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(findColumn(columnName));
    }

    public InputStream getAsciiStream(String columnName) {
        return (InputStream) get(columnName);
    }

    public InputStream getBinaryStream(String columnName) throws SQLFeatureNotSupportedException {
        return getBinaryStream(findColumn(columnName));
    }

    public Object getObject(int columnIndex) {
        return get(columnIndex);
    }

    public Object getObject(String columnName) {
        return get(columnName);
    }

    public JSONObject getJSONObject(String colName) {
        return getJSONObject(findColumn(colName));
    }

    public JSONObject getJSONObject(int columnIndex) {
        return (JSONObject) get(columnIndex);
    }

    public UUID getUUID(int columnIndex) {
        Object u = get(columnIndex);
        if (u == null) {
            return null;
        }
        return UUID.fromString((String) u);
    }

    public UUID getUUID(String columnName) {
        return getUUID(findColumn(columnName));
    }

    public int findColumn(String columnName) {
        return columns.get(columnName).getOrdinal();
    }

    public Reader getCharacterStream(int columnIndex) {
        return new StringReader(getString(columnIndex));
    }

    public Reader getCharacterStream(String columnName) {
        return new StringReader(getString(columnName));
    }

    public BigDecimal getBigDecimal(int columnIndex) {
        if (get(columnIndex) == null) {
            return null;
        }

        return new BigDecimal(getString(columnIndex));
    }

    public BigDecimal getBigDecimal(String columnName) {
        return getBigDecimal(findColumn(columnName));
    }

    public boolean isBeforeFirst() {
        return currentRow == null;
    }

    public boolean isAfterLast() {
        return currentRow == null && !rowIterator.hasNext();
    }

    public boolean isLast() {
        return currentRow != null && !rowIterator.hasNext();
    }

    public void beforeFirst() {
        rowIterator = rows.iterator();
    }

    public boolean first() {
        if (rows.isEmpty())
            return false;
        rowIterator = rows.iterator();
        currentRow = rowIterator.next();
        return true;
    }

    public boolean last() {
        if (rows.isEmpty())
            return false;
        while (rowIterator.next() != null)
            ;
        return true;
    }

    public boolean relative(int columnIndex) {
        return false;
    }

    public Array getArray(int columnIndex) {
        return (Array) get(columnIndex);
    }

    public Array getArray(String columnName) {
        return getArray(findColumn(columnName));
    }

    /*
     * This will return the full dateTime from Kusto as sql.Date is less precise
     */
    public LocalDateTime getKustoDateTime(int columnIndex) {
        if (get(columnIndex) == null) {
            return null;
        }
        String dateString = getString(columnIndex);
        return LocalDateTime.parse(dateString, kustoDateTimeFormatter);
    }

    public LocalDateTime getKustoDateTime(String columnName) {
        return getKustoDateTime(findColumn(columnName));
    }

    /**
     * This will cut the date up to yyyy-MM-dd'T'HH:mm:ss.SSS
     *
     * @param columnIndex         Column index that contains the date
     * @param calendar            Calendar container appropriate timezone
     * @throws SQLException        throws SQLException if date can't be parsed
     * @return Date
     */
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
        if (calendar == null) {
            return getDate(columnIndex);
        }

        switch (columnsAsArray[columnIndex].getColumnType()) {
            case "string":
            case "datetime":
                try {
                    if (get(columnIndex) == null) {
                        return null;
                    }
                    String dateString = getString(columnIndex);
                    FastDateFormat dateFormat;
                    if (dateString.length() < 21) {
                        dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss", calendar.getTimeZone());
                    } else {
                        dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS", calendar.getTimeZone());
                    }
                    return new java.sql.Date(dateFormat.parse(dateString.substring(0, Math.min(dateString.length() - 1, 23))).getTime());
                } catch (Exception e) {
                    throw new SQLException("Error parsing Date", e);
                }
            case "long":
            case "int":
                Long longVal = getLongObject(columnIndex);
                if (longVal == null) {
                    return null;
                }
                return new Date(longVal);
        }
        throw new SQLException("Error parsing Date - expected string, long or datetime data type.");
    }

    public Date getDate(String columnName, Calendar calendar) throws SQLException {
        return getDate(findColumn(columnName));
    }

    public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
        return getTime(columnIndex);
    }

    public Time getTime(String columnName, Calendar calendar) throws SQLException {
        return getTime(columnName);
    }

    public LocalTime getLocalTime(int columnIndex) {
        Object time = get(columnIndex);
        if (time == null) {
            return null;
        }
        return LocalTime.parse((String) time);
    }

    public LocalTime getLocalTime(String columnName) {
        return getLocalTime(findColumn(columnName));
    }

    public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
        return getTimestamp(columnIndex);
    }

    public Timestamp getTimestamp(String columnName, Calendar calendar) throws SQLException {
        return getTimestamp(findColumn(columnName), calendar);
    }

    public URL getURL(int columnIndex) throws SQLException {
        try {
            return new URL(getString(columnIndex));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    public URL getURL(String columnName) throws SQLException {
        try {
            return new URL(getString(columnName));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    public int count() {
        return rows.size();
    }

    public boolean isNull(int columnIndex) {
        return get(columnIndex) == null;
    }
}
