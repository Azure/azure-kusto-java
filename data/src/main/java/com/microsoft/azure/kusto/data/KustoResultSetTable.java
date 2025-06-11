// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.microsoft.azure.kusto.data.exceptions.JsonPropertyMissingException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

import static com.microsoft.azure.kusto.data.KustoOperationResult.ONE_API_ERRORS_PROPERTY_NAME;

// This class does not keep an open connection with the cluster - the results are evaluated once and can be retrieved using getData()
public class KustoResultSetTable {
    protected static final String TABLE_NAME_PROPERTY_NAME = "TableName";
    protected static final String TABLE_ID_PROPERTY_NAME = "TableId";
    protected static final String TABLE_KIND_PROPERTY_NAME = "TableKind";
    protected static final String COLUMNS_PROPERTY_NAME = "Columns";
    protected static final String COLUMN_NAME_PROPERTY_NAME = "ColumnName";
    protected static final String COLUMN_TYPE_PROPERTY_NAME = "ColumnType";
    protected static final String COLUMN_TYPE_SECOND_PROPERTY_NAME = "DataType";
    protected static final String ROWS_PROPERTY_NAME = "Rows";
    protected static final String EXCEPTIONS_PROPERTY_NAME = "Exceptions";

    private static final String EMPTY_STRING = "";
    private static final DateTimeFormatter kustoDateTimeFormatter = new DateTimeFormatterBuilder().parseCaseInsensitive()
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

    protected KustoResultSetTable(JsonNode jsonTable) {
        if (jsonTable.has(TABLE_NAME_PROPERTY_NAME)) {
            tableName = jsonTable.get(TABLE_NAME_PROPERTY_NAME).asText();
        }
        if (jsonTable.has(TABLE_ID_PROPERTY_NAME)) {
            tableId = jsonTable.get(TABLE_ID_PROPERTY_NAME).asText();
        }
        tableId = jsonTable.has(TABLE_ID_PROPERTY_NAME) ? jsonTable.get(TABLE_ID_PROPERTY_NAME).asText() : EMPTY_STRING;
        String tableKindString = jsonTable.has(TABLE_KIND_PROPERTY_NAME) ? jsonTable.get(TABLE_KIND_PROPERTY_NAME).asText() : EMPTY_STRING;
        tableKind = StringUtils.isBlank(tableKindString) ? null : WellKnownDataSet.valueOf(tableKindString);

        if (jsonTable.has(COLUMNS_PROPERTY_NAME) && jsonTable.get(COLUMNS_PROPERTY_NAME).getNodeType() == JsonNodeType.ARRAY) {
            ArrayNode columnsJson = (ArrayNode) jsonTable.get(COLUMNS_PROPERTY_NAME);
            if (columnsJson != null) {
                columnsAsArray = new KustoResultColumn[columnsJson.size()];
                for (int i = 0; i < columnsJson.size(); i++) {
                    JsonNode jsonCol = columnsJson.get(i);

                    // TODO yischoen Use CslFormat classes to validate columnType is valid, and that its value ("JsonNode obj" below) is valid for columnType
                    String columnType = jsonCol.has(COLUMN_TYPE_PROPERTY_NAME) ? jsonCol.get(COLUMN_TYPE_PROPERTY_NAME).asText() : EMPTY_STRING;
                    if (columnType.isEmpty()) {
                        columnType = jsonCol.has(COLUMN_TYPE_SECOND_PROPERTY_NAME) ? jsonCol.get(COLUMN_TYPE_SECOND_PROPERTY_NAME).asText() : EMPTY_STRING;
                    }
                    if (jsonCol.has(COLUMN_NAME_PROPERTY_NAME)) {
                        KustoResultColumn col = new KustoResultColumn(
                                jsonCol.has(COLUMN_NAME_PROPERTY_NAME) ? jsonCol.get(COLUMN_NAME_PROPERTY_NAME).asText() : EMPTY_STRING, columnType, i);
                        columnsAsArray[i] = col;
                        columns.put(jsonCol.has(COLUMN_NAME_PROPERTY_NAME) ? jsonCol.get(COLUMN_NAME_PROPERTY_NAME).asText() : EMPTY_STRING, col);
                    } else {
                        throw new JsonPropertyMissingException("Column Name property is missing in the json response");
                    }
                }
            }
        }

        ArrayNode exceptions;
        ArrayNode jsonRows = null;
        if (jsonTable.has(ROWS_PROPERTY_NAME) && jsonTable.get(ROWS_PROPERTY_NAME).getNodeType() == JsonNodeType.ARRAY) {
            jsonRows = (ArrayNode) jsonTable.get(ROWS_PROPERTY_NAME);
        }
        if (jsonRows != null) {
            List<List<Object>> values = new ArrayList<>();
            for (int i = 0; i < jsonRows.size(); i++) {
                JsonNode row = jsonRows.get(i);
                if (jsonRows.get(i).getNodeType() == JsonNodeType.OBJECT) {
                    exceptions = row.has(EXCEPTIONS_PROPERTY_NAME) ? ((ArrayNode) row.get(EXCEPTIONS_PROPERTY_NAME)) : null;
                    if (exceptions != null) {
                        throw KustoServiceQueryError.fromOneApiErrorArray(exceptions, exceptions.size() == 1); // TODO: this is the same logic as before, should
                                                                                                               // check with Yehezkel why isOneApi error is true
                                                                                                               // if there is one exception
                    } else {
                        throw KustoServiceQueryError.fromOneApiErrorArray((ArrayNode) row.get(ONE_API_ERRORS_PROPERTY_NAME), true);
                    }
                }
                ArrayNode rowAsJsonArray = (ArrayNode) jsonRows.get(i);
                List<Object> rowVector = new ArrayList<>();
                for (int j = 0; j < rowAsJsonArray.size(); j++) {
                    JsonNode obj = rowAsJsonArray.get(j);
                    if (obj.isNull()) {
                        rowVector.add(null);
                    } else {
                        switch (rowAsJsonArray.get(j).getNodeType()) {
                            case STRING:
                                rowVector.add(obj.asText());
                                break;
                            case BOOLEAN:
                                rowVector.add(obj.asBoolean());
                                break;
                            case NUMBER:
                                if (obj.isInt()) {
                                    rowVector.add(obj.asInt());
                                } else if (obj.isLong()) {
                                    rowVector.add(obj.asLong());
                                } else if (obj.isBigDecimal()) {
                                    rowVector.add(obj.decimalValue());
                                } else if (obj.isDouble()) {
                                    rowVector.add(obj.asDouble());
                                } else if (obj.isShort()) {
                                    rowVector.add(obj.shortValue());
                                } else if (obj.isFloat()) {
                                    rowVector.add(obj.floatValue());
                                } else {
                                    rowVector.add(obj);
                                }
                                break;
                            default:
                                rowVector.add(obj);
                        }
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

    private Object get(String columnName) {
        return currentRow.get(findColumn(columnName));
    }

    public String getString(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj == null) {
            return null;
        }

        return obj.toString();
    }

    public boolean getBoolean(int columnIndex) {
        return (boolean) get(columnIndex);
    }

    public Boolean getBooleanObject(int columnIndex) {
        return (Boolean) get(columnIndex);
    }

    public byte getByte(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj instanceof Integer) {
            return ((Integer) obj).byteValue();
        }
        return (byte) obj;
    }

    private Object getShortGeneric(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj instanceof Integer) {
            return ((Integer) obj).shortValue();
        }
        return obj;
    }

    public short getShort(int columnIndex) {
        return (short) getShortGeneric(columnIndex);
    }

    public Short getShortObject(int columnIndex) {
        return (Short) getShortGeneric(columnIndex);
    }

    public int getInt(int columnIndex) {
        return (int) get(columnIndex);
    }

    public Integer getIntegerObject(int columnIndex) {
        return (Integer) get(columnIndex);
    }

    private Object getLongGeneric(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj instanceof Integer) {
            return ((Integer) obj).longValue();
        }
        return obj;
    }

    public long getLong(int columnIndex) {
        return (long) getLongGeneric(columnIndex);
    }

    public Long getLongObject(int columnIndex) {
        return (Long) getLongGeneric(columnIndex);
    }

    private Object getFloatGeneric(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).floatValue();
        }
        return obj;
    }

    public float getFloat(int columnIndex) {
        return (float) getFloatGeneric(columnIndex);
    }

    public Float getFloatObject(int columnIndex) {
        return (Float) getFloatGeneric(columnIndex);
    }

    private Object getDoubleGeneric(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).doubleValue();
        }
        return obj;
    }

    public double getDouble(int columnIndex) {
        return (double) getDoubleGeneric(columnIndex);
    }

    public Double getDoubleObject(int columnIndex) {
        return (Double) getDoubleGeneric(columnIndex);
    }

    public byte[] getBytes(int columnIndex) {
        Object obj = get(columnIndex);
        if (obj instanceof String) {
            return ((String) obj).getBytes();
        }
        return (byte[]) obj;
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
        return getString(findColumn(columnName));
    }

    public boolean getBoolean(String columnName) {
        return getBoolean(findColumn(columnName));
    }

    public Boolean getBooleanObject(String columnName) {
        return getBooleanObject(findColumn(columnName));
    }

    public byte getByte(String columnName) {
        return getByte(findColumn(columnName));
    }

    public short getShort(String columnName) {
        return getShort(findColumn(columnName));
    }

    public Short getShortObject(String columnName) {
        return getShortObject(findColumn(columnName));
    }

    public int getInt(String columnName) {
        return getInt(findColumn(columnName));
    }

    public Integer getIntegerObject(String columnName) {
        return getIntegerObject(findColumn(columnName));
    }

    public long getLong(String columnName) {
        return getLong(findColumn(columnName));
    }

    public Long getLongObject(String columnName) {
        return getLongObject(findColumn(columnName));
    }

    public float getFloat(String columnName) {
        return getFloat(findColumn(columnName));
    }

    public Float getFloatObject(String columnName) {
        return getFloatObject(findColumn(columnName));
    }

    public double getDouble(String columnName) {
        return getDouble(findColumn(columnName));
    }

    public Double getDoubleObject(String columnName) {
        return getDoubleObject(findColumn(columnName));
    }

    public byte[] getBytes(String columnName) {
        return getBytes(findColumn(columnName));
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

    public JsonNode getJSONObject(String columnName) {
        return getJSONObject(findColumn(columnName));
    }

    public JsonNode getJSONObject(int columnIndex) {
        return (JsonNode) get(columnIndex);
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
        Object obj = get(columnIndex);
        if (obj == null) {
            return null;
        }

        return new BigDecimal(obj.toString());
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
                    
                    // First try the original FastDateFormat approach with strict patterns
                    try {
                        DateTimeFormatter formatter;
                        if (dateString.length() < 21) {
                            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
                                    .withZone(calendar.getTimeZone().toZoneId());
                        } else {
                            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
                                    .withZone(calendar.getTimeZone().toZoneId());
                        }
                        // Remove trailing 'Z' if present, similar to original FastDateFormat implementation
                        String parsableString = dateString.substring(0, Math.min(dateString.length() - (dateString.endsWith("Z") ? 1 : 0), 23));
                        Instant instant = formatter.parse(parsableString, Instant::from);
                        return new java.sql.Date(Date.from(instant).getTime());
                    } catch (Exception parseEx) {
                        // If strict parsing fails, try ISO parsing for properly formatted ISO strings
                        if (dateString.endsWith("Z")) {
                            try {
                                Instant instant = Instant.parse(dateString);
                                return new java.sql.Date(instant.toEpochMilli());
                            } catch (Exception isoEx) {
                                // If both approaches fail, try normalizing the malformed string and parsing again
                                try {
                                    String normalizedString = normalizeMalformedDateTime(dateString);
                                    Instant instant = Instant.parse(normalizedString);
                                    return new java.sql.Date(instant.toEpochMilli());
                                } catch (Exception normalizeEx) {
                                    // If all approaches fail, fall back to the original exception
                                    throw parseEx;
                                }
                            }
                        } else {
                            // If it doesn't end with Z, re-throw the original parsing exception
                            throw parseEx;
                        }
                    }
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

    /**
     * Helper method to normalize malformed datetime strings to make them parseable by Instant.parse()
     * This handles common malformations like single-digit seconds, missing digits, etc.
     */
    private String normalizeMalformedDateTime(String dateString) {
        if (dateString == null || !dateString.endsWith("Z")) {
            return dateString;
        }
        
        // Remove the 'Z' to work with the datetime part
        String datePart = dateString.substring(0, dateString.length() - 1);
        
        // Split into date and time parts
        String[] parts = datePart.split("T");
        if (parts.length != 2) {
            return dateString; // Can't normalize if format is too different
        }
        
        String datePortion = parts[0]; // yyyy-MM-dd
        String timePortion = parts[1]; // HH:mm:ss or HH:mm:ss.SSS...
        
        // Split time into components
        String[] timeComponents = timePortion.split(":");
        if (timeComponents.length != 3) {
            return dateString; // Can't normalize if time format is wrong
        }
        
        String hours = timeComponents[0];
        String minutes = timeComponents[1];
        String secondsAndFractions = timeComponents[2];
        
        // Pad hours and minutes to 2 digits
        hours = String.format("%02d", Integer.parseInt(hours));
        minutes = String.format("%02d", Integer.parseInt(minutes));
        
        // Handle seconds and fractions
        String[] secondsParts = secondsAndFractions.split("\\.");
        String seconds = secondsParts[0];
        
        // Pad seconds to 2 digits
        seconds = String.format("%02d", Integer.parseInt(seconds));
        
        // Reconstruct the normalized datetime string
        String normalizedTime = hours + ":" + minutes + ":" + seconds;
        if (secondsParts.length > 1) {
            // Preserve fractional seconds
            normalizedTime += "." + secondsParts[1];
        }
        
        return datePortion + "T" + normalizedTime + "Z";
    }
}
