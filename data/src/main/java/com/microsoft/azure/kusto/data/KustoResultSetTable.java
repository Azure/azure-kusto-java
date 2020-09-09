// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.KustoServiceError;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

// This class does not keep an open connection with the cluster - the results are evaluated once and can be get by getData()
public class KustoResultSetTable implements ResultSet {
    private static final String tableNamePropertyName = "TableName";
    private static final String tableIdPropertyName = "TableId";
    private static final String tableKindPropertyName = "TableKind";
    private static final String columnsPropertyName = "Columns";
    private static final String columnNamePropertyName = "ColumnName";
    private static final String columnTypePropertyName = "ColumnType";
    private static final String columnTypeSecondPropertyName = "DataType";
    private static final String rowsPropertyName = "Rows";
    private static final String exceptionsPropertyName = "Exceptions";

    private ArrayList<ArrayList<Object>> rows = null;
    private String tableName;
    private String tableId;
    private WellKnownDataSet tableKind;
    private Map<String, KustoResultColumn> columns = new HashMap<>();
    private KustoResultColumn[] columnsAsArray = null;
    private Iterator<ArrayList<Object>> iterator = Collections.emptyIterator();
    private ArrayList<Object> current = null;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableId() {
        return tableId;
    }

    public KustoResultColumn[] getColumns(){
        return columnsAsArray;
    }
    void setTableId(String tableId) {
        this.tableId = tableId;
    }

    void setTableKind(WellKnownDataSet tableKind) {
        this.tableKind = tableKind;
    }

    WellKnownDataSet getTableKind() {
        return tableKind;
    }

    KustoResultSetTable(JSONObject jsonTable) throws JSONException, KustoServiceError {
        tableName = jsonTable.optString(tableNamePropertyName);
        tableId = jsonTable.optString(tableIdPropertyName);
        String tableKindString = jsonTable.optString(tableKindPropertyName);
        tableKind = StringUtils.isBlank(tableKindString) ? null : WellKnownDataSet.valueOf(tableKindString);
        JSONArray columnsJson = jsonTable.optJSONArray(columnsPropertyName);
        if (columnsJson != null) {
            columnsAsArray = new KustoResultColumn[columnsJson.length()];
            for (int i = 0; i < columnsJson.length(); i++) {
                JSONObject jsonCol = columnsJson.getJSONObject(i);
                String columnType = jsonCol.optString(columnTypePropertyName);
                if (columnType.equals("")){
                    columnType = jsonCol.optString(columnTypeSecondPropertyName);
                }
                KustoResultColumn col = new KustoResultColumn(jsonCol.getString(columnNamePropertyName), columnType, i);
                columnsAsArray[i] = col;
                columns.put(jsonCol.getString(columnNamePropertyName), col);
            }
        }

        JSONArray exceptions;
        JSONArray jsonRows = jsonTable.optJSONArray(rowsPropertyName);
        if (jsonRows != null) {
            ArrayList<ArrayList<Object>> values = new ArrayList<>();
            for (int i = 0; i < jsonRows.length(); i++) {
                Object row = jsonRows.get(i);
                if (row instanceof JSONObject) {
                    exceptions = ((JSONObject) row).optJSONArray(exceptionsPropertyName);
                    if (exceptions != null) {
                        if (exceptions.length() == 1) {
                            String message = exceptions.getString(0);
                            throw new KustoServiceError(message);
                        } else {
                            throw new KustoServiceError(exceptions);
                        }
                    }
                }
                JSONArray rowAsJsonArray = jsonRows.getJSONArray(i);
                ArrayList<Object> rowVector = new ArrayList<>();
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

        iterator = rows.iterator();
    }

    public ArrayList<Object> getCurrentRow(){
        return current;
    }

    @Override
    public boolean next() {
        boolean hasNext = iterator.hasNext();
        if(hasNext) {
            current = iterator.next();
        }
        return hasNext;
    }

    public ArrayList<ArrayList<Object>> getData(){
        return rows;
    }

    @Override
    public void close() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Kusto resultSet is not closeable as there is no open connection");
    }

    @Override
    public boolean wasNull() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    private Object get(int i){
        return current.get(i);
    }

    private Object get(String colName){
        return current.get(findColumn(colName));
    }

    @Override
    public String getString(int i)  {
        return get(i).toString();
    }

    @Override
    public boolean getBoolean(int i)  {
        return (boolean) get(i);
    }

    @Override
    public byte getByte(int i)  {
        return (byte) get(i);
    }

    @Override
    public short getShort(int i)  {
        return (short) get(i);
    }

    @Override
    public int getInt(int i)  {
        return (int) get(i);
    }

    @Override
    public long getLong(int i)  {
        Object obj = get(i);
        if(obj instanceof Integer){
            return ((Integer)obj).longValue();
        }
        return (long) obj;
    }

    @Override
    public float getFloat(int i)  {
        return (float) get(i); }

    @Override
    public double getDouble(int i)  {
        return (double) get(i);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int i, int i1)  {
        return (BigDecimal) get(i);
    }

    @Override
    public byte[] getBytes(int i)  {
        return (byte[]) get(i);
    }

    @Override
    public Date getDate(int i) throws SQLException {
        return getDate(i, Calendar.getInstance());
    }

    @Override
    public Time getTime(int i) throws SQLException {
        return new Time(getDate(i).getTime());
    }

    @Override
    public Timestamp getTimestamp(int i) throws SQLException {
        switch (columnsAsArray[i].getColumnType()) {
            case "string":
            case "datetime":
                return Timestamp.valueOf(StringUtils.chop(getString(i)).replace("T"," "));
            case "long":
            case "int":
                return new Timestamp(getLong(i));

        }
        throw new SQLException("Error parsing timestamp - expected string or long columns.");
    }

    @Override
    public InputStream getAsciiStream(int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public InputStream getBinaryStream(int i) throws SQLFeatureNotSupportedException {
        if(columnsAsArray[i].getColumnType().equals("String")) {
            return new ByteArrayInputStream(getString(i).getBytes());
        }

        throw new SQLFeatureNotSupportedException("getBinaryStream is only available for strings");
    }

    @Override
    public String getString(String columnName)  {
        return get(columnName).toString();
    }

    @Override
    public boolean getBoolean(String columnName)  {
        return (boolean) get(columnName);
    }

    @Override
    public byte getByte(String columnName)  {
        return (byte) get(columnName);
    }

    @Override
    public short getShort(String columnName) {
        return (short) get(columnName);
    }

    @Override
    public int getInt(String columnName) {
        return (int) get(columnName);
    }

    @Override
    public long getLong(String columnName) {
        return (long) get(columnName);
    }

    @Override
    public float getFloat(String columnName) {
        return (float) get(columnName);
    }

    @Override
    public double getDouble(String columnName) {
        return (double) get(columnName);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnName, int i) {
        return (BigDecimal) get(columnName);
    }

    @Override
    public byte[] getBytes(String columnName) {
        return (byte[]) get(columnName);
    }

    @Override
    public Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName));
    }

    @Override
    public Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName));
    }

    @Override
    public Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(findColumn(columnName));
    }

    @Override
    public InputStream getAsciiStream(String columnName) {
        return (InputStream) get(columnName);
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnName) {
        return (InputStream) get(columnName);
    }

    @Override
    public InputStream getBinaryStream(String columnName) throws SQLFeatureNotSupportedException {
        return getBinaryStream(findColumn(columnName));
    }

    @Override
    public SQLWarning getWarnings() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void clearWarnings() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public String getCursorName() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Object getObject(int i) {
        return get(i);
    }

    @Override
    public Object getObject(String columnName) {
        return get(columnName);
    }

    public JSONObject getJSONObject(String colName){
        return getJSONObject(findColumn(colName));
    }

    public JSONObject getJSONObject(int i){
        return (JSONObject) get(i);
    }

    @Override
    public int findColumn(String columnName){
        return columns.get(columnName).getOrdinal();
    }

    @Override
    public Reader getCharacterStream(int i) {
        return new StringReader(getString(i));
    }

    @Override
    public Reader getCharacterStream(String columnName) {
        return new StringReader(getString(columnName));
    }

    @Override
    public BigDecimal getBigDecimal(int i) {
        return new BigDecimal(getString(i));
    }

    @Override
    public BigDecimal getBigDecimal(String columnName) {
        return getBigDecimal(findColumn(columnName));
    }

    @Override
    public boolean isBeforeFirst() {
        return current == null;
    }

    @Override
    public boolean isAfterLast() {
        return current == null && !iterator.hasNext();
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public boolean isLast() {
        return current !=null && !iterator.hasNext();
    }

    @Override
    public void beforeFirst() {
        iterator = rows.iterator();
    }

    @Override
    public void afterLast() {
        while(next());
    }

    @Override
    public boolean first() {
        if (rows.size() == 0)
            return false;
        iterator = rows.iterator();
        current = iterator.next();
        return true;
    }

    @Override
    public boolean last() {
        if (rows.size() == 0)
            return false;
        while(iterator.next() != null);
        return true;
    }

    // This means the row number in the Kusto database and therefore is irrelevant to Kusto
    @Override
    public int getRow() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public boolean absolute(int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public boolean relative(int i) {
        return false;
    }

    @Override
    public boolean previous() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setFetchDirection(int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public int getFetchDirection() {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int i) {

    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getType() {
        return TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() {
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean rowDeleted() {
        return false;
    }

    @Override
    public void updateNull(int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateBoolean(int i, boolean b) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateByte(int i, byte b) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateShort(int i, short i1) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateInt(int i, int i1) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateLong(int i, long l) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateFloat(int i, float v) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateDouble(int i, double v) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateString(int i, String columnName) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateBytes(int i, byte[] bytes) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateDate(int i, Date date) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateTime(int i, Time time) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateTimestamp(int i, Timestamp timestamp) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, int i1) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, int i1) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateCharacterStream(int i, Reader reader, int i1) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateObject(int i, Object o, int i1) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateObject(int i, Object o) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateNull(String columnName) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateBoolean(String columnName, boolean b) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateByte(String columnName, byte b) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateShort(String columnName, short i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateInt(String columnName, int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateLong(String columnName, long l) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateFloat(String columnName, float v) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateDouble(String columnName, double v) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateBigDecimal(String columnName, BigDecimal bigDecimal) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateString(String columnName, String s1) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateBytes(String columnName, byte[] bytes) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateDate(String columnName, Date date) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateTime(String columnName, Time time) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateTimestamp(String columnName, Timestamp timestamp) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateAsciiStream(String columnName, InputStream inputStream, int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateBinaryStream(String columnName, InputStream inputStream, int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateCharacterStream(String columnName, Reader reader, int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateObject(String columnName, Object o, int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateObject(String columnName, Object o) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void insertRow() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateRow() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void deleteRow() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void refreshRow() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void cancelRowUpdates() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void moveToInsertRow() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void moveToCurrentRow() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Statement getStatement() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Ref getRef(int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Blob getBlob(int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Clob getClob(int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Array getArray(int i) {
        return (Array) get(i);
    }

    @Override
    public Object getObject(String columnName, Map<String, Class<?>> map) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Ref getRef(String columnName) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Blob getBlob(String columnName) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Clob getClob(String columnName) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public Array getArray(String columnName) {
        return getArray(findColumn(columnName));
    }

    /*
     * This will return the full dateTime from Kusto as sql.Date is less precise
     */
    public LocalDateTime getKustoDateTime(int i) {
        String dateString = getString(i);
        DateTimeFormatter dateTimeFormatter;
        if(dateString.length() < 21){
            dateTimeFormatter = new DateTimeFormatterBuilder().parseCaseInsensitive().append(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")).toFormatter();
        } else {
            dateTimeFormatter = new DateTimeFormatterBuilder().parseCaseInsensitive().append(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'")).toFormatter();
        }
        return LocalDateTime.parse(getString(i), dateTimeFormatter);
    }

    public LocalDateTime getKustoDateTime(String columnName) {
        return getKustoDateTime(findColumn(columnName));
    }

    /**
     * This will cut the date up to yyyy-MM-dd'T'HH:mm:ss.SSS
     */
    @Override
    public Date getDate(int i, Calendar calendar) throws SQLException {
        if (calendar == null) {
            return getDate(i);
        }

        switch (columnsAsArray[i].getColumnType()) {
            case "string":
            case "datetime":
                try {
                    String dateString = getString(i);
                    FastDateFormat dateFormat;
                    if(dateString.length() < 21){
                        dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss", calendar.getTimeZone());
                    } else {
                        dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS", calendar.getTimeZone());
                    }
                    return new java.sql.Date(dateFormat.parse(dateString.substring(0, Math.min(dateString.length() - 1, 23))).getTime());
                }
                catch (Exception e) {
                    throw new SQLException("Error parsing Date");
                }
            case "long":
            case "int":
                return new Date(getLong(i));
        }
        throw new SQLException("Error parsing Date - expected string, long or datetime data type.");      }

    @Override
    public Date getDate(String columnName, Calendar calendar) throws SQLException {
        return getDate(findColumn(columnName));
    }

    @Override
    public Time getTime(int i, Calendar calendar) throws SQLException {
        return new Time(getDate(i, calendar).getTime());
    }

    @Override
    public Time getTime(String columnName, Calendar calendar) throws SQLException {
        return getTime(findColumn(columnName), calendar);
    }

    @Override
    public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
        return getTimestamp(i);
    }

    @Override
    public Timestamp getTimestamp(String columnName, Calendar calendar) throws SQLException {
        return getTimestamp(findColumn(columnName), calendar);
    }

    @Override
    public URL getURL(int i) throws SQLException {
        try {
            return new URL(getString(i));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public URL getURL(String columnName) throws SQLException {
        try {
            return new URL(getString(columnName));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void updateRef(int i, Ref ref) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");

    }

    @Override
    public void updateRef(String columnName, Ref ref) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");

    }

    @Override
    public void updateBlob(int i, Blob blob) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");

    }

    @Override
    public void updateBlob(String columnName, Blob blob) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");

    }

    @Override
    public void updateClob(int i, Clob clob) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");

    }

    @Override
    public void updateClob(String columnName, Clob clob) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");

    }

    @Override
    public void updateArray(int i, Array array) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");

    }

    @Override
    public void updateArray(String columnName, Array array) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public RowId getRowId(int i) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public RowId getRowId(String columnName) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateRowId(int i, RowId rowId) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void updateRowId(String columnName, RowId rowId) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void updateNString(int i, String s) {

    }

    @Override
    public void updateNString(String columnName, String s1) {

    }

    @Override
    public void updateNClob(int i, NClob nClob) {

    }

    @Override
    public void updateNClob(String columnName, NClob nClob) {

    }

    @Override
    public NClob getNClob(int i) {
        return null;
    }

    @Override
    public NClob getNClob(String columnName) {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int i) {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnName) {
        return null;
    }

    @Override
    public void updateSQLXML(int i, SQLXML sqlxml) {

    }

    @Override
    public void updateSQLXML(String columnName, SQLXML sqlxml) {

    }

    @Override
    public String getNString(int i) {
        return null;
    }

    @Override
    public String getNString(String columnName) {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int i) {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnName) {
        return null;
    }

    @Override
    public void updateNCharacterStream(int i, Reader reader, long l) {

    }

    @Override
    public void updateNCharacterStream(String columnName, Reader reader, long l) {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, long l) {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, long l) {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader, long l) {

    }

    @Override
    public void updateAsciiStream(String columnName, InputStream inputStream, long l) {

    }

    @Override
    public void updateBinaryStream(String columnName, InputStream inputStream, long l) {

    }

    @Override
    public void updateCharacterStream(String columnName, Reader reader, long l) {

    }

    @Override
    public void updateBlob(int i, InputStream inputStream, long l) {

    }

    @Override
    public void updateBlob(String columnName, InputStream inputStream, long l) {

    }

    @Override
    public void updateClob(int i, Reader reader, long l) {

    }

    @Override
    public void updateClob(String columnName, Reader reader, long l) {

    }

    @Override
    public void updateNClob(int i, Reader reader, long l) {

    }

    @Override
    public void updateNClob(String columnName, Reader reader, long l) {

    }

    @Override
    public void updateNCharacterStream(int i, Reader reader) {

    }

    @Override
    public void updateNCharacterStream(String columnName, Reader reader) {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream) {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream) {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader) {

    }

    @Override
    public void updateAsciiStream(String columnName, InputStream inputStream) {

    }

    @Override
    public void updateBinaryStream(String columnName, InputStream inputStream) {

    }

    @Override
    public void updateCharacterStream(String columnName, Reader reader) {

    }

    @Override
    public void updateBlob(int i, InputStream inputStream) {

    }

    @Override
    public void updateBlob(String columnName, InputStream inputStream) {

    }

    @Override
    public void updateClob(int i, Reader reader) {

    }

    @Override
    public void updateClob(String columnName, Reader reader) {

    }

    @Override
    public void updateNClob(int i, Reader reader) {

    }

    @Override
    public void updateNClob(String columnName, Reader reader) {

    }

    @Override
    public <T> T getObject(int i, Class<T> aClass) {
        return null;
    }

    @Override
    public <T> T getObject(String columnName, Class<T> aClass) {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) {
        return false;
    }

    public int count(){
        return rows.size();
    }
}
