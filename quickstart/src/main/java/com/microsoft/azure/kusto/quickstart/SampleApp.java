package com.microsoft.azure.kusto.quickstart;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Scanner;


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
    private static boolean waitForUser;


    public static void main(String[] args) {
        System.out.println("Kusto sample app is starting...");

        ConfigJson config = loadConfigs();
        waitForUser = config.isWaitForUser();

        if (config.getAuthenticationMode() == AuthenticationModeOptions.userPrompt) {
            waitForUserToProceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.");
        }
        try {
            IngestClient ingestClient = IngestClientFactory.createClient(Utils.Authentication.generateConnectionString(config.getIngestUri(),
                    config.getAuthenticationMode()));
            Client kustoClient = ClientFactory.createClient(Utils.Authentication.generateConnectionString(config.getKustoUri(), config.getAuthenticationMode()));

            preIngestionQuerying(config, kustoClient);


        } catch (URISyntaxException e) {
            Utils.errorHandler("Couldn't create client. Please validate your URIs in the configuration file.", e);
        }

        System.out.println("\nKusto sample app done");

    }

    /**
     * Loads JSON configuration file, and sets the metadata in place
     *
     * @return ConfigJson object, allowing access to the metadata fields
     */
    @NotNull
    private static ConfigJson loadConfigs() {
        File configFile = new File(".\\" + SampleApp.configFileName);
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(configFile, ConfigJson.class);

        } catch (Exception e) {
            Utils.errorHandler(String.format("Couldn't read config file from file '%s'", SampleApp.configFileName), e);
        }
        return new ConfigJson(); //TODO: will never reach here. what should i replace with?
    }

    /**
     * Handles UX on prompts and flow of program
     *
     * @param promptMsg Prompt to display to user
     */
    private static void waitForUserToProceed(String promptMsg) {
        System.out.println();
        System.out.printf("\nStep %s: %s%n", step++, promptMsg);
        if (waitForUser) {
            System.out.println("Press ENTER to proceed with this operation...");
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }
    }

    /**
     * First phase, pre ingestion - will reach the provided DB with several control commands and a query based on the configuration File.
     *
     * @param config      ConfigJson object containing the SampleApp configuration
     * @param kustoClient Client to run commands
     */
    private static void preIngestionQuerying(ConfigJson config, Client kustoClient) {
        if (config.isUseExistingTable()) {
            if (config.isAlterTable()) {
                // Tip: Usually table was originally created with a schema appropriate for the data being ingested, so this wouldn't be needed.
                // Learn More: For more information about altering table schemas, see:
                // https://docs.microsoft.com/azure/data-explorer/kusto/management/alter-table-command
                waitForUserToProceed(String.format("Alter-merge existing table '%s.%s' to align with the provided schema", config.getDatabaseName(),
                        config.getTableName()));
                alterMergeExistingTableToProvidedSchema(kustoClient, config.getDatabaseName(), config.getTableName(), config.getTableSchema());
            }
            if (config.isQueryData()) {
                waitForUserToProceed(String.format("Get existing row count in '%s.%s'", config.getDatabaseName(), config.getTableName()));
                queryExistingNumberOfRows(kustoClient, config.getDatabaseName(), config.getTableName());
            }
        } else {
            // Tip: This is generally a one-time configuration
            // Learn More: For more information about creating tables, see: https://docs.microsoft.com/azure/data-explorer/one-click-table
            waitForUserToProceed(String.format("Create table '%s.%s'", config.getDatabaseName(), config.getTableName()));
            createNewTable(kustoClient, config.getDatabaseName(), config.getTableName(), config.getTableSchema());
        }

        // Learn More: Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
        //   1) More than 1,000 files were queued for ingestion for the same table by the same user
        //   2) More than 1GB of data was queued for ingestion for the same table by the same user
        //   3) More than 5 minutes have passed since the first File was queued for ingestion for the same table by the same user
        //  For more information about customizing the ingestion batching policy, see:
        // https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy
        // TODO: Change if needed. Disabled to prevent an existing batching policy from being unintentionally changed
        if (false && config.getBatchingPolicy() != null) {
            waitForUserToProceed(String.format("Alter the batching policy for table '%s.%s'", config.getDatabaseName(), config.getTableName()));
            alterBatchingPolicy(kustoClient, config.getDatabaseName(), config.getTableName(), config.getBatchingPolicy());
        }
    }

    /**
     * Alter-merges the given existing table to provided schema
     *
     * @param kustoClient  Client to run commands
     * @param databaseName DB name
     * @param tableName    Table name
     * @param tableSchema  Table Schema
     */
    private static void alterMergeExistingTableToProvidedSchema(Client kustoClient, String databaseName, String tableName, String tableSchema) {
        String command = String.format(".alter-merge table %s %s", tableName, tableSchema);
        Utils.Queries.executeCommand(kustoClient, databaseName, command);
    }

    /**
     * Queries the data on the existing number of rows
     *
     * @param kustoClient  Client to run commands
     * @param databaseName DB name
     * @param tableName    Table name
     */
    private static void queryExistingNumberOfRows(Client kustoClient, String databaseName, String tableName) {
        String command = String.format("%s | count", tableName);
        Utils.Queries.executeCommand(kustoClient, databaseName, command);
    }

    /**
     * Creates a new table
     *
     * @param kustoClient  Client to run commands
     * @param databaseName DB name
     * @param tableName    Table name
     * @param tableSchema  Table Schema
     */
    private static void createNewTable(Client kustoClient, String databaseName, String tableName, String tableSchema) {
        String command = String.format(".create table %s %s", tableName, tableSchema);
        Utils.Queries.executeCommand(kustoClient, databaseName, command);
    }

    /**
     * Alters the batching policy based on BatchingPolicy in configuration
     *
     * @param kustoClient    Client to run commands
     * @param databaseName   DB name
     * @param tableName      Table name
     * @param batchingPolicy Ingestion batching policy
     */
    private static void alterBatchingPolicy(Client kustoClient, String databaseName, String tableName, String batchingPolicy) {
        /*
         * Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during development and in this sample app, we opt to modify
         * the default ingestion policy to ingest data after at most 10 seconds.
         * Tip 2: This is generally a one-time configuration.
         * Tip 3: You can also skip the batching for some files using the Flush-Immediately property, though this option should be used with care as it
         * is inefficient.
         */
        String command = String.format(".alter table %s policy ingestionbatching @'%s'", tableName, batchingPolicy);
        Utils.Queries.executeCommand(kustoClient, databaseName, command);
        // If it failed to alter the ingestion policy - it could be the result of insufficient permissions. The sample will still run,
        // though ingestion will be delayed for up to 5 minutes.
    }

}