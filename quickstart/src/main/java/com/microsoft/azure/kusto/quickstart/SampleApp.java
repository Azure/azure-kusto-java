package com.microsoft.azure.kusto.quickstart;

import com.azure.core.tracing.opentelemetry.OpenTelemetryTracer;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.data.http.HttpPostUtils;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.Tracer;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.net.URISyntaxException;
import java.util.*;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporters.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

/**
 * SourceType - represents the type of files used for ingestion
 */
enum SourceType {
    LOCAL_FILE_SOURCE("localFileSource"), BLOB_SOURCE("blobSource"), NO_SOURCE("nosource");

    private final String source;

    SourceType(String source) {
        this.source = source;
    }

    public static SourceType valueOfLabel(String label) {
        for (SourceType e : values()) {
            if (e.source.equals(label)) {
                return e;
            }
        }
        return null;
    }
}

/**
 * AuthenticationModeOptions - represents the different options to authenticate to the system
 */
enum AuthenticationModeOptions {
    USER_PROMPT("UserPrompt"), MANAGED_IDENTITY("ManagedIdentity"), APP_KEY("AppKey"), APP_CERTIFICATE("AppCertificate");

    private final String mode;

    AuthenticationModeOptions(String mode) {
        this.mode = mode;
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
@JsonIgnoreProperties(ignoreUnknown = true)
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
        this.sourceType = SourceType.valueOfLabel(sourceType);
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
                ", \ndataFormat=" + format +
                ", \nuseExistingMapping=" + useExistingMapping +
                ", \nmappingName='" + mappingName + '\'' +
                ", \nmappingValue='" + mappingValue + '\'' +
                "}\n";
    }
}

/**
 * ConfigJson object - represents a cluster and DataBase connection configuration file.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
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
    /// Some auth modes require additional environment variables to be set in order to work (see usage in generate_connection_string function).
    /// Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
    private AuthenticationModeOptions authenticationMode;
    /// Recommended default: True
    /// Toggle to False to execute this script "unattended"
    private boolean waitForUser;
    /// Ignores the first record in a "X-seperated value" type file
    private boolean ignoreFirstRecord;
    /// Sleep time to allow for queued ingestion to complete.
    private int waitForIngestSeconds;
    /// Optional - Customized ingestion batching policy
    private String batchingPolicy;

    public boolean isUseExistingTable() {
        return useExistingTable;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public String getKustoUri() {
        return kustoUri;
    }

    public String getIngestUri() {
        return ingestUri;
    }

    public List<ConfigData> getData() {
        return data;
    }

    public boolean isAlterTable() {
        return alterTable;
    }

    public boolean isQueryData() {
        return queryData;
    }

    public boolean isIngestData() {
        return ingestData;
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

    public boolean isIgnoreFirstRecord() {
        return ignoreFirstRecord;
    }

    public int getWaitForIngestSeconds() {
        return waitForIngestSeconds;
    }

    public String getBatchingPolicy() {
        return batchingPolicy;
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
                ", \nignoreFirstRecord=" + ignoreFirstRecord +
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
        // TODO (tracing): Uncomment the following line to enable tracing.
        // initializeTracing();

        MonitoredActivity.invoke(SampleApp::runSampleApp, "SampleApp.runSampleApp");

    }

    private static void runSampleApp() {
        System.out.println("Kusto sample app is starting...");
        ConfigJson config = loadConfigs();
        waitForUser = config.isWaitForUser();

        if (config.getAuthenticationMode() == AuthenticationModeOptions.USER_PROMPT) {
            waitForUserToProceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.");
        }
        try {
            IngestClient ingestClient = IngestClientFactory.createClient(Utils.Authentication.generateConnectionString(config.getIngestUri(),
                    config.getAuthenticationMode()));
            Client kustoClient = ClientFactory
                    .createClient(Utils.Authentication.generateConnectionString(config.getKustoUri(), config.getAuthenticationMode()));

            preIngestionQuerying(config, kustoClient);

            if (config.isIngestData()) {
                ingestion(config, kustoClient, ingestClient);
            }
            if (config.isQueryData()) {
                postIngestionQuerying(kustoClient, config.getDatabaseName(), config.getTableName(), config.isIngestData());
            }

        } catch (URISyntaxException e) {
            Utils.errorHandler("Couldn't create client. Please validate your URIs in the configuration file.", e);
        }
        System.out.println("\nKusto sample app done");
    }

    /**
     * Initializes the OpenTelemetry SDK with default values and registers it as the {@link com.microsoft.azure.kusto.data.instrumentation.Tracer}
     */
    private static void initializeTracing() {
        enableDistributedTracing();
        Tracer.initializeTracer(new OpenTelemetryTracer());
    }

    /**
     * Configures the OpenTelemetry SDK with default values and registers it as the {@link io.opentelemetry.api.GlobalOpenTelemetry}.
     */
    private static void enableDistributedTracing() {
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "logical-service-name")));
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(new LoggingSpanExporter()).build())
                .setResource(resource)
                .build();
        OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .buildAndRegisterGlobal();
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
            ObjectMapper mapper = HttpPostUtils.getObjectMapper();
            mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);
            return mapper.readValue(configFile, ConfigJson.class);

        } catch (Exception e) {
            Utils.errorHandler(String.format("Couldn't read config file from file '%s'", SampleApp.configFileName), e);
        }
        return new ConfigJson(); // Note: will never reach here.
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
        // 1) More than 1,000 files were queued for ingestion for the same table by the same user
        // 2) More than 1GB of data was queued for ingestion for the same table by the same user
        // 3) More than 5 minutes have passed since the first File was queued for ingestion for the same table by the same user
        // For more information about customizing the ingestion batching policy, see:
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
        String command = String.format(".alter-merge table %s %s", StringUtils.normalizeEntityName(tableName), tableSchema);
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
        String command = String.format("%s | count", StringUtils.normalizeEntityName(tableName));
        Utils.Queries.executeCommand(kustoClient, databaseName, command);
    }

    /**
     * Queries the first two rows of the table
     *
     * @param kustoClient  Client to run commands
     * @param databaseName DB name
     * @param tableName    Table name
     */
    private static void queryFirstTwoRows(Client kustoClient, String databaseName, String tableName) {
        String command = String.format("%s | take 2", StringUtils.normalizeEntityName(tableName));
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
        String command = String.format(".create table %s %s", StringUtils.normalizeEntityName(tableName), tableSchema);
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
         * the default ingestion policy to ingest data after at most 10 seconds. Tip 2: This is generally a one-time configuration. Tip 3: You can also skip the
         * batching for some files using the Flush-Immediately property, though this option should be used with care as it is inefficient.
         */
        String command = String.format(".alter table %s policy ingestionbatching @'%s'", StringUtils.normalizeEntityName(tableName), batchingPolicy);
        Utils.Queries.executeCommand(kustoClient, databaseName, command);
        // If it failed to alter the ingestion policy - it could be the result of insufficient permissions. The sample will still run,
        // though ingestion will be delayed for up to 5 minutes.
    }

    /**
     * Second phase - The ingestion process
     *
     * @param config       ConfigJson object containing the SampleApp configuration
     * @param kustoClient  Client to run commands
     * @param ingestClient Client to ingest data
     */
    private static void ingestion(ConfigJson config, Client kustoClient, IngestClient ingestClient) {
        for (ConfigData dataSource : config.getData()) {
            // Tip: This is generally a one-time configuration.
            // Learn More: For more information about providing inline mappings and mapping references, see:
            // https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings
            createIngestionMappings(dataSource.isUseExistingMapping(), kustoClient, config.getDatabaseName(), config.getTableName(),
                    dataSource.getMappingName(), dataSource.getMappingValue(), dataSource.getFormat());

            // Learn More: For more information about ingesting data to Kusto in Java, see:
            // https://docs.microsoft.com/azure/data-explorer/java-ingest-data
            ingest_data(dataSource, dataSource.getFormat(), ingestClient, config.getDatabaseName(), config.getTableName(), dataSource.getMappingName(),
                    config.isIgnoreFirstRecord());
        }

        /*
         * Note: We poll here the ingestion's target table because monitoring successful ingestions is expensive and not recommended. Instead, the recommended
         * ingestion monitoring approach is to monitor failures. Learn more:
         * https://docs.microsoft.com/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-status#tracking-ingestion-status-kustoqueuedingestclient and
         * https://docs.microsoft.com/azure/data-explorer/using-diagnostic-logs
         */
        Utils.Ingestion.waitForIngestionToComplete(config.getWaitForIngestSeconds());
    }

    /**
     * Creates Ingestion Mappings (if required) based on given values
     *
     * @param useExistingMapping Flag noting if we should the existing mapping or create a new one
     * @param kustoClient        Client to run commands
     * @param databaseName       DB name
     * @param tableName          Table name
     * @param mappingName        Desired mapping name
     * @param mappingValue       Values of the new mappings to create
     * @param dataFormat         Given data format
     */
    private static void createIngestionMappings(boolean useExistingMapping, Client kustoClient, String databaseName, String tableName, String mappingName,
            String mappingValue, IngestionProperties.DataFormat dataFormat) {
        if (useExistingMapping || StringUtils.isBlank(mappingValue)) {
            return;
        }
        IngestionMapping.IngestionMappingKind ingestionMappingKind = dataFormat.getIngestionMappingKind();
        waitForUserToProceed(String.format("Create a '%s' mapping reference named '%s'", ingestionMappingKind.getKustoValue(), mappingName));
        mappingName = StringUtils.isNotBlank(mappingName) ? mappingName : "DefaultQuickstartMapping" + UUID.randomUUID().toString().substring(0, 5);

        String mappingCommand = String.format(".create-or-alter table %s ingestion %s mapping '%s' '%s'", StringUtils.normalizeEntityName(tableName),
                ingestionMappingKind.getKustoValue().toLowerCase(), mappingName, mappingValue);
        Utils.Queries.executeCommand(kustoClient, databaseName, mappingCommand);
    }

    /**
     * Ingest data from given source
     *
     * @param data_source       Given data source
     * @param dataFormat        Given data format
     * @param ingestClient      Client to ingest data
     * @param databaseName      DB name
     * @param tableName         Table name
     * @param mappingName       Desired mapping name
     * @param ignoreFirstRecord Flag noting whether to ignore the first record in the table
     */
    private static void ingest_data(ConfigData data_source, IngestionProperties.DataFormat dataFormat, IngestClient ingestClient, String databaseName,
            String tableName, String mappingName, boolean ignoreFirstRecord) {
        SourceType sourceType = data_source.getSourceType();
        String uri = data_source.getDataSourceUri();
        waitForUserToProceed(String.format("Ingest '%s' from '%s'", uri, sourceType.toString()));
        // Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even if the file only contains one line.
        // If the json contains whitespace formatting, use SINGLEJSON. In this case, only one data row json object is allowed per file.
        if (dataFormat == IngestionProperties.DataFormat.JSON) {
            dataFormat = IngestionProperties.DataFormat.MULTIJSON;
        }

        // Tip: Kusto's Java SDK can ingest data from files, blobs, java.sql.ResultSet objects, and open streams.
        // See the SDK's kusto-samples module and the E2E tests in kusto-ingest for additional references.
        // Note: No need to add "nosource" option as in that case the "ingestData" flag will be set to false, and it will be imppossible to reach this code
        // segemnt.
        switch (sourceType) {
            case LOCAL_FILE_SOURCE:
                Utils.Ingestion.ingestFromFile(ingestClient, databaseName, tableName, uri, dataFormat, mappingName, ignoreFirstRecord);
                break;
            case BLOB_SOURCE:
                Utils.Ingestion.ingestFromBlob(ingestClient, databaseName, tableName, uri, dataFormat, mappingName, ignoreFirstRecord);
                break;
            default:
                Utils.errorHandler(String.format("Unknown source '%s' for file '%s'%n", sourceType, uri));
        }
    }

    /**
     * Third and final phase - simple queries to validate the hopefully successful run of the script
     *
     * @param kustoClient  Client to run queries
     * @param databaseName DB Name
     * @param tableName    Table Name
     * @param ingestData   Flag noting whether any data was ingested by the script
     */
    private static void postIngestionQuerying(Client kustoClient, String databaseName, String tableName, boolean ingestData) {
        String optionalPostIngestionPrompt = ingestData ? "post-ingestion " : "";

        waitForUserToProceed(String.format("Get %srow count for '%s.%s':", optionalPostIngestionPrompt, databaseName, tableName));
        queryExistingNumberOfRows(kustoClient, databaseName, tableName);

        waitForUserToProceed(String.format("Get sample (2 records) of %sdata:", optionalPostIngestionPrompt));
        queryFirstTwoRows(kustoClient, databaseName, tableName);
    }
}
