package com.microsoft.azure.kusto.ingest;

import java.util.HashMap;
import java.util.Map;

public class IngestionProperties {
    private String databaseName;
    private String tableName;
    private boolean flushImmediately;
    private IngestionReportLevel reportLevel;
    private IngestionReportMethod reportMethod;
    private Map<String, String> additionalProperties;

    public enum DATA_FORMAT {csv,tsv,scsv,sohsv,psv,txt,json,singlejson,avro,parquet}

    public String getDatabaseName() { return databaseName; }

    public String getTableName() { return tableName; }

    public boolean getFlushImmediately() { return flushImmediately; }

    public IngestionReportLevel getReportLevel() { return reportLevel; }

    public IngestionReportMethod getReportMethod() { return reportMethod; }

    public Map<String, String> getAdditionalProperties() { return additionalProperties; }

    public void setFlushImmediately(boolean flushImmediately) { this.flushImmediately = flushImmediately; }

    public void setReportLevel(IngestionReportLevel reportLevel) { this.reportLevel = reportLevel; }

    public void setReportMethod(IngestionReportMethod reportMethod) { this.reportMethod = reportMethod; }

    public void setDataFormat(DATA_FORMAT dataFormat)
    {
        additionalProperties.put("format", dataFormat.name());
    }

    /**
     * Sets the data format by its name. If the name does not exist, then it does not add it.
     * @param dataFormatName
     */
    public void setDataFormat(String dataFormatName)
    {
        String dataFormat = DATA_FORMAT.valueOf(dataFormatName.toLowerCase()).name();
        if(!dataFormat.isEmpty()){
            additionalProperties.put("format", dataFormat);
        }
    }

    public void setJsonMappingName(String jsonMappingName)
    {
        additionalProperties.put("jsonMappingReference", jsonMappingName);
        additionalProperties.put("format", DATA_FORMAT.json.name());
    }

    public void setCsvMappingName(String mappingName)
    {
        additionalProperties.put("csvMappingReference", mappingName);
        additionalProperties.put("format", DATA_FORMAT.csv.name());
    }

    public void setAuthorizationContextToken(String token)
    {
        additionalProperties.put("authorizationContext", token);
    }

    public IngestionProperties(String databaseName, String tableName)
    {
        this.databaseName = databaseName;
        this.tableName = tableName;
        reportLevel = IngestionReportLevel.FailuresOnly;
        reportMethod = IngestionReportMethod.Queue;
        flushImmediately = false;
        additionalProperties = new HashMap();
    }

    public enum IngestionReportLevel
    {
        FailuresOnly,
        None,
        FailuresAndSuccesses
    }

    public enum IngestionReportMethod
    {
        Queue,
        Table,
        QueueAndTable
    }
}