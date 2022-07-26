package com.microsoft.azure.kusto.quickstart;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.ingest.IngestionProperties;

import java.io.File;
import java.util.List;

import static com.microsoft.azure.kusto.quickstart.Utils.*;

/**
 * SourceType - represents the type of files used for ingestion
 */
enum SourceType {
    localFileSource, blobSource
}

/**
 * AuthenticationModeOptions - represents the different options to autenticate to the system
 */
enum AuthenticationModeOptions {
    userPrompt("UserPrompt"), managedIdentity("ManagedIdentity"), appKey("AppKey"), appCertificate("AppCertificate");

    private String mode;

    AuthenticationModeOptions(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static AuthenticationModeOptions valueOfLabel(String label) {
        for (AuthenticationModeOptions e : values()) {
            if (e.mode.equals(label)) {
                return e;
            }
        }
        return null;
    }
}

/**
 * ConfigData object - represents a file from which to ingest
 */
class ConfigData {
    private SourceType sourceType;
    private String dataSourceUri;
    private IngestionProperties.DataFormat format;
    private boolean useExistingMapping;
    private String mappingName;
    private String mappingValue;

    public SourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = SourceType.valueOf(sourceType);
    }

    public String getDataSourceUri() {
        return dataSourceUri;
    }

    public void setDataSourceUri(String dataSourceUri) {
        this.dataSourceUri = dataSourceUri;
    }

    public IngestionProperties.DataFormat getFormat() {
        return format;
    }

    public void setFormat(IngestionProperties.DataFormat format) {
        this.format = format;
    }

    public boolean isUseExistingMapping() {
        return useExistingMapping;
    }

    public void setUseExistingMapping(boolean useExistingMapping) {
        this.useExistingMapping = useExistingMapping;
    }

    public String getMappingName() {
        return mappingName;
    }

    public void setMappingName(String mappingName) {
        this.mappingName = mappingName;
    }

    public String getMappingValue() {
        return mappingValue;
    }

    public void setMappingValue(String mappingValue) {
        this.mappingValue = mappingValue;
    }

    @Override
    public String toString() {
        return "\nConfigData{" +
                "\nsourceType=" + sourceType +
                ", \ndataSourceUri='" + dataSourceUri + '\'' +
                ", \nformat=" + format +
                ", \nuseExistingMapping=" + useExistingMapping +
                ", \nmappingName='" + mappingName + '\'' +
                ", \nmappingValue='" + mappingValue + '\'' +
                "}\n";
    }
}

/**
 * ConfigJson object - represents a cluster and DataBase connection configuration file.
 */
class ConfigJson {
    private boolean useExistingTable;
    private String databaseName;
    private String tableName;
    private String tableSchema;
    private String kustoUri;
    private String ingestUri;
    private String tenantId;
    private List<ConfigData> data;
    private boolean alterTable;
    private boolean queryData;
    private boolean ingestData;
    /// Recommended default: UserPrompt
    /// Some of the auth modes require additional environment variables to be set in order to work (see usage in generate_connection_string function).
    /// Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
    private AuthenticationModeOptions authenticationMode;
    /// Recommended default: True
    /// Toggle to False to execute this script "unattended"
    private boolean waitForUser;
    /// Sleep time to allow for queued ingestion to complete.
    private int waitForIngestSeconds;
    /// Optional - Customized ingestion batching policy
    private String batchingPolicy;

    public boolean isUseExistingTable() {
        return useExistingTable;
    }

    public void setUseExistingTable(boolean useExistingTable) {
        this.useExistingTable = useExistingTable;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getKustoUri() {
        return kustoUri;
    }

    public void setKustoUri(String kustoUri) {
        this.kustoUri = kustoUri;
    }

    public String getIngestUri() {
        return ingestUri;
    }

    public void setIngestUri(String ingestUri) {
        this.ingestUri = ingestUri;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public List<ConfigData> getData() {
        return data;
    }

    public void setData(List<ConfigData> data) {
        this.data = data;
    }

    public boolean isAlterTable() {
        return alterTable;
    }

    public void setAlterTable(boolean alterTable) {
        this.alterTable = alterTable;
    }

    public boolean isQueryData() {
        return queryData;
    }

    public void setQueryData(boolean queryData) {
        this.queryData = queryData;
    }

    public boolean isIngestData() {
        return ingestData;
    }

    public void setIngestData(boolean ingestData) {
        this.ingestData = ingestData;
    }

    public AuthenticationModeOptions getAuthenticationMode() {
        return authenticationMode;
    }

    public void setAuthenticationMode(String authenticationMode) {
        this.authenticationMode = AuthenticationModeOptions.valueOfLabel(authenticationMode);
    }

    public boolean isWaitForUser() {
        return waitForUser;
    }

    public void setWaitForUser(boolean waitForUser) {
        this.waitForUser = waitForUser;
    }

    public int getWaitForIngestSeconds() {
        return waitForIngestSeconds;
    }

    public void setWaitForIngestSeconds(int waitForIngestSeconds) {
        this.waitForIngestSeconds = waitForIngestSeconds;
    }

    public String getBatchingPolicy() {
        return batchingPolicy;
    }

    public void setBatchingPolicy(String batchingPolicy) {
        this.batchingPolicy = batchingPolicy;
    }

    @Override
    public String toString() {
        return "ConfigJson{" +
                "\nuseExistingTable=" + useExistingTable +
                ", \ndatabaseName='" + databaseName + '\'' +
                ", \ntableName='" + tableName + '\'' +
                ", \ntableSchema='" + tableSchema + '\'' +
                ", \nkustoUri='" + kustoUri + '\'' +
                ", \ningestUri='" + ingestUri + '\'' +
                ", \ntenantId='" + tenantId + '\'' +
                ", \ndata=" + data +
                ", \nalterTable=" + alterTable +
                ", \nqueryData=" + queryData +
                ", \ningestData=" + ingestData +
                ", \nauthenticationMode=" + authenticationMode +
                ", \nwaitForUser=" + waitForUser +
                ", \nwaitForIngestSeconds=" + waitForIngestSeconds +
                ", \nbatchingPolicy='" + batchingPolicy + '\'' +
                "}\n";
    }
}

/**
 * The quick start application is a self-contained and runnable example script that demonstrates authenticating connecting to, administering, ingesting
 * data into and querying Azure Data Explorer using the azure-kusto C# SDK. You can use it as a baseline to write your own first kusto client application,
 * altering the code as you go, or copy code sections out of it into your app.
 * Tip: The app includes comments with tips on recommendations, coding best practices, links to reference materials and recommended TO DO changes when
 * adapting the code to your needs.
 */
public class SampleApp {
    // TODO (config):
    // If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated with your cluster's details.
    // If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL and database fields appropriately.
    private static final String configFileName = "quickstart/kusto_sample_config.json";
    private static int step = 1;

    public static void main(String[] args) {
        System.out.println("Kusto sample app is starting...");

        ConfigJson config = LoadConfigs();

        System.out.println("\nKusto sample app done");

    }

    /**
     * Loads JSON configuration file, and sets the metadata in place
     *
     * @return ConfigJson object, allowing access to the metadata fields
     */
    private static ConfigJson LoadConfigs() {
        File configFile = new File(".\\" + SampleApp.configFileName);
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(configFile, ConfigJson.class);

        } catch (Exception e) {
            ErrorHandler(String.format("Couldn't read config file from file '%s'", SampleApp.configFileName), e);
        }
        return null;
    }
}
