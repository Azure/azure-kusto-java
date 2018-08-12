package com.microsoft.azure.kusto.ingest;

import java.util.HashMap;
import java.util.Map;

public class KustoIngestionProperties {
    private String m_databaseName;
    private String m_tableName;
    private boolean m_flushImmediately;
    private IngestionReportLevel m_reportLevel;
    private IngestionReportMethod m_reportMethod;
    private Map<String, String> m_additionalProperties;

    public enum DATA_FORMAT {csv,tsv,scsv,sohsv,psv,txt,json,singlejson,avro,parquet}

    public String getDatabaseName() { return m_databaseName; }

    public String getTableName() { return m_tableName; }

    public boolean getFlushImmediately() { return m_flushImmediately; }

    public IngestionReportLevel getReportLevel() { return m_reportLevel; }

    public IngestionReportMethod getReportMethod() { return m_reportMethod; }

    public Map<String, String> getAdditionalProperties() { return m_additionalProperties; }

    public void setFlushImmediately(boolean flushImmediately) { m_flushImmediately = flushImmediately; }

    public void setReportLevel(IngestionReportLevel reportLevel) { m_reportLevel = reportLevel; }

    public void setReportMethod(IngestionReportMethod reportMethod) { m_reportMethod = reportMethod; }

    public void setDataFormat(DATA_FORMAT dataFormat)
    {
        m_additionalProperties.put("format", dataFormat.name());
    }

    /**
     * Sets the data format by its name. If the name does not exist, then it does not add it.
     * @param dataFormatName
     */
    public void setDataFormat(String dataFormatName)
    {
        String dataFormat = DATA_FORMAT.valueOf(dataFormatName.toLowerCase()).name();
        if(!dataFormat.isEmpty()){
            m_additionalProperties.put("format", dataFormat);
        }
    }

    public void setJsonMappingName(String jsonMappingName)
    {
        m_additionalProperties.put("jsonMappingReference", jsonMappingName);
        m_additionalProperties.put("format", DATA_FORMAT.json.name());
    }

    public void setCsvMappingName(String mappingName)
    {
        m_additionalProperties.put("csvMappingReference", mappingName);
        m_additionalProperties.put("format", DATA_FORMAT.csv.name());
    }

    public void setAuthorizationContextToken(String token)
    {
        m_additionalProperties.put("authorizationContext", token);
    }

    public KustoIngestionProperties(String databaseName, String tableName)
    {
        m_databaseName = databaseName;
        m_tableName = tableName;
        m_reportLevel = IngestionReportLevel.FailuresOnly;
        m_reportMethod = IngestionReportMethod.Queue;
        m_flushImmediately = false;
        m_additionalProperties = new HashMap();
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