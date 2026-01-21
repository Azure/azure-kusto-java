package com.microsoft.azure.kusto.quickstart;

import com.azure.core.tracing.opentelemetry.OpenTelemetryTracer;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.Tracer;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.IngestionOperation;
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache;
import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache;
import com.microsoft.azure.kusto.ingest.v2.common.SimpleRetryPolicy;
import com.microsoft.azure.kusto.ingest.v2.common.models.ClientDetails;
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.models.*;
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.FileSource;
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;
import com.microsoft.azure.kusto.ingest.v2.uploader.ManagedUploader;
import com.microsoft.azure.kusto.ingest.v2.uploader.UploadMethod;
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResult;
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResults;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporters.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.fasterxml.jackson.databind.MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS;

/**
 * SourceType - represents the type of files used for ingestion
 */
enum SourceType {
    LOCAL_FILE_SOURCE("localFileSource"), BLOB_SOURCE("blobSource"), NO_SOURCE("nosource");

    private final String source;

    SourceType(String source) {
        this.source = source;
    }

    public static @Nullable SourceType valueOfLabel(String label) {
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
    private AuthenticationModeOptions authenticationMode;
    private boolean waitForUser;
    private boolean ignoreFirstRecord;
    private int waitForIngestSeconds;
    private String batchingPolicy;
    private boolean useIngestV2Sample;
    private IngestV2QuickstartConfig ingestV2Config;

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

    public boolean isUseIngestV2Sample() {
        return useIngestV2Sample;
    }

    public IngestV2QuickstartConfig getIngestV2Config() {
        if (ingestV2Config == null) {
            ingestV2Config = new IngestV2QuickstartConfig();
        }
        return ingestV2Config;
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
                ", \nuseIngestV2Sample=" + useIngestV2Sample +
                ", \ningestV2Config=" + ingestV2Config +
                "}\n";
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class IngestV2QuickstartConfig {
    private String clusterPath;
    private final boolean trackingEnabled = true;
    private final int maxConcurrency = 10;
    private final int pollingIntervalSeconds = 30;
    private final int pollingTimeoutMinutes = 2;
    private final int overallTimeoutMinutes = 5;

    private AuthenticationModeOptions authModeOverride;
    private String appId;
    private String appKey;
    private String tenantId;
    private String dataMappingName;

    void applyDefaultsFromRoot(ConfigJson root) {
        if (StringUtils.isBlank(clusterPath)) {
            clusterPath = root.getKustoUri();
        }
        if (authModeOverride == null) {
            authModeOverride = root.getAuthenticationMode();
        }
        if (authModeOverride == AuthenticationModeOptions.APP_KEY) {
            if (StringUtils.isBlank(appId)) {
                appId = System.getenv("APP_ID");
            }
            if (StringUtils.isBlank(appKey)) {
                appKey = System.getenv("APP_KEY");
            }
            if (StringUtils.isBlank(tenantId)) {
                tenantId = System.getenv("APP_TENANT");
            }
        }
    }

    public String getClusterPath() {
        return clusterPath;
    }

    public AuthenticationModeOptions getAuthModeOverride() {
        return authModeOverride;
    }

    public String getAppId() {
        return appId;
    }

    public String getAppKey() {
        return appKey;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getDataMappingName() {
        return dataMappingName;
    }

    public boolean isTrackingEnabled() {
        return trackingEnabled;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public int getPollingIntervalSeconds() {
        return pollingIntervalSeconds;
    }

    public int getPollingTimeoutMinutes() {
        return pollingTimeoutMinutes;
    }

    public int getOverallTimeoutMinutes() {
        return overallTimeoutMinutes;
    }

    @Override
    public String toString() {
        return "IngestV2QuickstartConfig{" +
                "clusterPath='" + clusterPath + '\'' +
                ", authMode='" + authModeOverride + '\'' +
                ", appId='" + appId + '\'' +
                ", tenantId='" + tenantId + '\'' +
                ", dataMappingName='" + dataMappingName + '\'' +
                ", trackingEnabled=" + trackingEnabled +
                ", maxConcurrency=" + maxConcurrency +
                ", pollingIntervalSeconds=" + pollingIntervalSeconds +
                ", pollingTimeoutMinutes=" + pollingTimeoutMinutes +
                ", overallTimeoutMinutes=" + overallTimeoutMinutes +
                '}';
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
    private static final String CONFIG_ENV_OVERRIDE = "KUSTO_SAMPLE_CONFIG_PATH";
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
        IngestV2QuickstartConfig ingestV2Config = config.getIngestV2Config();
        ingestV2Config.applyDefaultsFromRoot(config);
        waitForUser = config.isWaitForUser();

        if (config.getAuthenticationMode() == AuthenticationModeOptions.USER_PROMPT) {
            waitForUserToProceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.");
        }

        if (config.isUseIngestV2Sample()) {
            runIngestV2Sample(config);
            return;
        }

        try {
            IngestClient ingestClient = IngestClientFactory.createClient(Utils.Authentication.generateConnectionString(config.getIngestUri(),
                    config.getAuthenticationMode()));
            Client kustoClient = ClientFactory
                    .createClient(Utils.Authentication.generateConnectionString(config.getKustoUri(), config.getAuthenticationMode()));

            preIngestionQuerying(config, kustoClient);

            if (config.isIngestData()) {
                ingest(config, kustoClient, ingestClient);
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
        Path configPath = locateConfigFile();
        try {
            ObjectMapper mapper = JsonMapper.builder().configure(ACCEPT_CASE_INSENSITIVE_ENUMS, true).build();
            return mapper.readValue(configPath.toFile(), ConfigJson.class);
        } catch (Exception e) {
            Utils.errorHandler(String.format("Couldn't read config file from file '%s'", configPath), e);
        }
        return new ConfigJson();
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
    private static void ingest(ConfigJson config, Client kustoClient, IngestClient ingestClient) {
        for (ConfigData dataSource : config.getData()) {
            // Tip: This is generally a one-time configuration.
            // Learn More: For more information about providing inline mappings and mapping references, see:
            // https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings
            createIngestionMappings(dataSource.isUseExistingMapping(), kustoClient, config.getDatabaseName(), config.getTableName(),
                    dataSource.getMappingName(), dataSource.getMappingValue(), dataSource.getFormat());

            // Learn More: For more information about ingesting data to Kusto in Java, see:
            // https://docs.microsoft.com/azure/data-explorer/java-ingest-data
            ingestData(dataSource, dataSource.getFormat(), ingestClient, config.getDatabaseName(), config.getTableName(), dataSource.getMappingName(),
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
     * @param dataSource       Given data source
     * @param dataFormat        Given data format
     * @param ingestClient      Client to ingest data
     * @param databaseName      DB name
     * @param tableName         Table name
     * @param mappingName       Desired mapping name
     * @param ignoreFirstRecord Flag noting whether to ignore the first record in the table
     */
    private static void ingestData(ConfigData dataSource, IngestionProperties.DataFormat dataFormat, IngestClient ingestClient, String databaseName,
            String tableName, String mappingName, boolean ignoreFirstRecord) {
        SourceType sourceType = dataSource.getSourceType();
        String uri = dataSource.getDataSourceUri();
        waitForUserToProceed(String.format("Ingest '%s' from '%s'", uri, sourceType.toString()));
        // Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even if the file only contains one line.
        // If the json contains whitespace formatting, use SINGLEJSON. In this case, only one data row json object is allowed per file.
        if (dataFormat == IngestionProperties.DataFormat.JSON) {
            dataFormat = IngestionProperties.DataFormat.MULTIJSON;
        }

        // Tip: Kusto's Java SDK can ingest data from files, blobs, java.sql.ResultSet objects, and open streams.
        // See the SDK's kusto-samples module and the E2E tests in kusto-ingest for additional references.
        // Note: No need to add "nosource" option as in that case the "ingestData" flag will be set to false, and it will be impossible to reach this code
        // segment.
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

    private static void runIngestV2Sample(@NotNull ConfigJson config) {
        IngestV2QuickstartConfig ingestV2Config = config.getIngestV2Config();
        String clusterPath = ingestV2Config.getClusterPath();
        if (StringUtils.isBlank(clusterPath)) {
            Utils.errorHandler("'kustoUri' must be provided to use the ingest-v2 sample.");
        }

        System.out.println("Running ingest-v2 quickstart sample...");
        TokenCredential credential = buildIngestV2Credential(ingestV2Config);

        try (QueuedIngestClient queuedIngestClient = QueuedIngestClientBuilder.create(clusterPath)
                .withAuthentication(credential)
                .withMaxConcurrency(ingestV2Config.getMaxConcurrency())
                .build()) {
            List<CompletableFuture<Void>> operations = new ArrayList<>();
            operations.addAll(ingestV2FromStreams(config, ingestV2Config, queuedIngestClient));
            operations.addAll(ingestV2FromFiles(config, ingestV2Config, queuedIngestClient));
            operations.add(ingestV2BatchIngestion(config, ingestV2Config, queuedIngestClient));

            CompletableFuture<Void> combined = CompletableFuture.allOf(operations.toArray(new CompletableFuture[0]));
            combined.get(ingestV2Config.getOverallTimeoutMinutes(), TimeUnit.MINUTES);
            System.out.println("All ingest-v2 operations completed successfully!");
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            Utils.errorHandler("Error running ingest-v2 quickstart sample", e);
        }
    }

    private static TokenCredential buildIngestV2Credential(@NotNull IngestV2QuickstartConfig config) {
        AuthenticationModeOptions mode = config.getAuthModeOverride();
        if (mode == null) {
            mode = AuthenticationModeOptions.USER_PROMPT;
        }
        if (mode == AuthenticationModeOptions.APP_KEY) {
            if (StringUtils.isBlank(config.getAppId()) || StringUtils.isBlank(config.getAppKey()) || StringUtils.isBlank(config.getTenantId())) {
                Utils.errorHandler("AppKey authentication requires 'APP_ID', 'APP_KEY', and 'APP_TENANT' environment variables or ingestV2 overrides.");
            }
            return new ClientSecretCredentialBuilder()
                    .clientId(config.getAppId())
                    .clientSecret(config.getAppKey())
                    .tenantId(config.getTenantId())
                    .build();
        } else {
            return new AzureCliCredentialBuilder().build();
        }
    }

    private static @NotNull List<CompletableFuture<Void>> ingestV2FromStreams(ConfigJson config, IngestV2QuickstartConfig ingestV2Config,
            @NotNull QueuedIngestClient queuedIngestClient) throws IOException {
        System.out.println("\n=== Queued ingestion from streams (ingest-v2) ===");
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        IngestRequestProperties csvProps = buildIngestV2RequestProperties(config, ingestV2Config, null);
        String csvData = "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
        InputStream csvStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(csvData).array());
        StreamSource csvSource = new StreamSource(csvStream, Format.csv, CompressionType.NONE, UUID.randomUUID(), false);
        futures.add(queuedIngestClient.ingestAsync(config.getDatabaseName(), config.getTableName(), csvSource, csvProps)
                .thenCompose(response -> {
                    closeQuietly(csvStream);
                    System.out.println("CSV stream ingestion queued. Operation ID: " + response.getIngestResponse().getIngestionOperationId());
                    return trackIngestV2Operation(config, ingestV2Config, queuedIngestClient, response, "CSV Stream");
                }));

        InputStream jsonStream = Files.newInputStream(resolveQuickstartPath("dataset.json"));
        StreamSource jsonSource = new StreamSource(jsonStream, Format.json, CompressionType.NONE, UUID.randomUUID(), false);
        IngestRequestProperties jsonProps = buildIngestV2RequestProperties(config, ingestV2Config, ingestV2Config.getDataMappingName());
        futures.add(queuedIngestClient.ingestAsync(config.getDatabaseName(), config.getTableName(), jsonSource, jsonProps)
                .thenCompose(response -> {
                    closeQuietly(jsonStream);
                    System.out.println("JSON stream ingestion queued. Operation ID: " + response.getIngestResponse().getIngestionOperationId());
                    return trackIngestV2Operation(config, ingestV2Config, queuedIngestClient, response, "JSON Stream");
                }));

        return futures;
    }

    private static @NotNull List<CompletableFuture<Void>> ingestV2FromFiles(ConfigJson config, IngestV2QuickstartConfig ingestV2Config,
            @NotNull QueuedIngestClient queuedIngestClient) {
        System.out.println("\n=== Queued ingestion from files (ingest-v2) ===");
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        IngestRequestProperties csvProps = buildIngestV2RequestProperties(config, ingestV2Config, null);
        FileSource csvFileSource = new FileSource(resolveQuickstartPath("dataset.csv"), Format.csv, UUID.randomUUID(), CompressionType.NONE);
        futures.add(queuedIngestClient.ingestAsync(config.getDatabaseName(), config.getTableName(), csvFileSource, csvProps)
                .thenCompose(response -> {
                    System.out.println("CSV file ingestion queued. Operation ID: " + response.getIngestResponse().getIngestionOperationId());
                    return trackIngestV2Operation(config, ingestV2Config, queuedIngestClient, response, "CSV File");
                }));

        FileSource jsonFileSource = new FileSource(resolveQuickstartPath("dataset.json"), Format.json, UUID.randomUUID(), CompressionType.NONE);
        IngestRequestProperties jsonProps = buildIngestV2RequestProperties(config, ingestV2Config, ingestV2Config.getDataMappingName());
        futures.add(queuedIngestClient.ingestAsync(config.getDatabaseName(), config.getTableName(), jsonFileSource, jsonProps)
                .thenCompose(response -> {
                    System.out.println("JSON file ingestion queued. Operation ID: " + response.getIngestResponse().getIngestionOperationId());
                    return trackIngestV2Operation(config, ingestV2Config, queuedIngestClient, response, "JSON File");
                }));

        return futures;
    }

    private static @NotNull CompletableFuture<Void> ingestV2BatchIngestion(ConfigJson config, IngestV2QuickstartConfig ingestV2Config,
                                                                           @NotNull QueuedIngestClient queuedIngestClient) {
        System.out.println("\n=== Queued batch ingestion: Upload local files to blob, then ingest (ingest-v2) ===");
        String clusterPath = ingestV2Config.getClusterPath();
        TokenCredential credential = buildIngestV2Credential(ingestV2Config);

        ConfigurationCache configCache = DefaultConfigurationCache.create(
                clusterPath,
                credential,
                new ClientDetails("SampleApp", "1.0", "quickstart-sample"));

        ManagedUploader uploader = ManagedUploader.builder()
                .withConfigurationCache(configCache)
                .withRetryPolicy(new SimpleRetryPolicy())
                .withMaxConcurrency(ingestV2Config.getMaxConcurrency())
                .withMaxDataSize(4L * 1024 * 1024 * 1024) // 4GB max size
                .withUploadMethod(UploadMethod.STORAGE)
                .withTokenCredential(credential)
                .build();

        System.out.println("ManagedUploader created for batch upload");

        FileSource source1 = new FileSource(resolveQuickstartPath("dataset.csv"), Format.csv, UUID.randomUUID(), CompressionType.NONE);
        FileSource source2 = new FileSource(resolveQuickstartPath("dataset.csv"), Format.csv, UUID.randomUUID(), CompressionType.NONE);
        List<LocalSource> localSources = Arrays.asList(source1, source2);

        System.out.println("Prepared " + localSources.size() + " local files for upload:");
        for (LocalSource source : localSources) {
            System.out.println("  - " + source.getName() + " (format: " + source.getFormat() + ")");
        }

        IngestRequestProperties props = buildIngestV2RequestProperties(config, ingestV2Config, null);


        System.out.println("Uploading " + localSources.size() + " files to blob storage...");

        return uploader.uploadManyAsync(localSources)
                .thenCompose(uploadResults -> {
                    System.out.println("Upload completed:");
                    System.out.println("  Successes: " + uploadResults.getSuccesses().size());
                    System.out.println("  Failures: " + uploadResults.getFailures().size());

                    for (UploadResult.Failure failure : uploadResults.getFailures()) {
                        System.err.println("  Upload failed for " + failure.getSourceName()
                                + ": " + failure.getErrorMessage());
                    }

                    List<BlobSource> blobSources = new ArrayList<>();
                    for (UploadResult.Success success : uploadResults.getSuccesses()) {
                        System.out.println("  Uploaded: " + success.getSourceName()
                                + " -> " + success.getBlobUrl().split("\\?")[0]); // Hide SAS token in log

                        BlobSource blobSource = new BlobSource(
                                success.getBlobUrl(),
                                Format.csv,  // All our files are CSV format
                                UUID.randomUUID(),
                                CompressionType.GZIP);
                        blobSources.add(blobSource);
                    }

                    if (blobSources.isEmpty()) {
                        return CompletableFuture.<Void>failedFuture(
                                new RuntimeException("All uploads failed - nothing to ingest"));
                    }

                    System.out.println("Ingesting " + blobSources.size() + " blobs as a batch...");
                    return queuedIngestClient.ingestAsync(config.getDatabaseName(), config.getTableName(), blobSources, props)
                            .thenCompose(response -> {
                                System.out.println("Batch ingestion queued. Operation ID: "
                                        + response.getIngestResponse().getIngestionOperationId());
                                System.out.println("Number of sources in batch: " + blobSources.size());
                                return trackIngestV2Operation(config, ingestV2Config, queuedIngestClient, response, "Batch Upload & Ingest");
                            });
                })
                .whenComplete((unused, throwable) -> {
                    uploader.close();
                    System.out.println("ManagedUploader closed");
                });
    }

    private static @NotNull IngestRequestProperties buildIngestV2RequestProperties(@NotNull ConfigJson config, @NotNull IngestV2QuickstartConfig ingestV2Config,
            String mappingName) {
        IngestRequestPropertiesBuilder builder = IngestRequestPropertiesBuilder
                .create()
                .withEnableTracking(ingestV2Config.isTrackingEnabled());
        if (StringUtils.isNotBlank(mappingName)) {
            // Only JSON samples are shown in the sample
            com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping mapping = new com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping(
                    mappingName,
                    com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping.IngestionMappingType.JSON);
            builder.withIngestionMapping(mapping);
        }
        return builder.build();
    }

    private static @NotNull CompletableFuture<Void> trackIngestV2Operation(@NotNull ConfigJson config, @NotNull IngestV2QuickstartConfig ingestV2Config,
            @NotNull QueuedIngestClient queuedIngestClient, @NotNull ExtendedIngestResponse response, String operationName) {
        IngestionOperation operation = new IngestionOperation(
                Objects.requireNonNull(response.getIngestResponse().getIngestionOperationId()),
                config.getDatabaseName(),
                config.getTableName(),
                response.getIngestionType());

        Duration pollInterval = Duration.ofSeconds(ingestV2Config.getPollingIntervalSeconds());
        Duration pollTimeout = Duration.ofMinutes(ingestV2Config.getPollingTimeoutMinutes());

        System.out.println("\n--- Tracking " + operationName + " ---");
        return queuedIngestClient.getOperationDetailsAsync(operation)
                .thenCompose(initialDetails -> {
                    System.out.println("[" + operationName + "] Initial Operation Details:");
                    printIngestV2StatusResponse(initialDetails);
                    System.out.println("[" + operationName + "] Polling for completion...");
                    return queuedIngestClient.pollForCompletion(operation, pollInterval, pollTimeout);
                })
                .thenCompose(fin -> queuedIngestClient.getOperationDetailsAsync(operation))
                .thenAccept(finalDetails -> {
                    System.out.println("[" + operationName + "] Final Operation Details:");
                    printIngestV2StatusResponse(finalDetails);
                    System.out.println("[" + operationName + "] Operation tracking completed.\n");
                })
                .exceptionally(error -> {
                    System.err.println("[" + operationName + "] Error tracking operation: " + error.getMessage());
                    error.printStackTrace();
                    return null;
                });
    }

    private static void printIngestV2StatusResponse(StatusResponse statusResponse) {
        if (statusResponse == null) {
            System.out.println("  Status: null");
            return;
        }
        Status status = statusResponse.getStatus();
        if (status != null) {
            System.out.println("  Summary:");
            System.out.println("    In Progress: " + status.getInProgress());
            System.out.println("    Succeeded: " + status.getSucceeded());
            System.out.println("    Failed: " + status.getFailed());
            System.out.println("    Canceled: " + status.getCanceled());
        }
        List<BlobStatus> details = statusResponse.getDetails();
        if (details != null && !details.isEmpty()) {
            System.out.println("  Blob Details:");
            for (int i = 0; i < details.size(); i++) {
                BlobStatus blobStatus = details.get(i);
                System.out.println("    Blob " + (i + 1) + ":");
                System.out.println("      Source ID: " + blobStatus.getSourceId());
                System.out.println("      Status: " + blobStatus.getStatus());
                if (blobStatus.getDetails() != null) {
                    System.out.println("      Details: " + blobStatus.getDetails());
                }
                if (blobStatus.getErrorCode() != null) {
                    System.out.println("      Error Code: " + blobStatus.getErrorCode());
                }
                if (blobStatus.getFailureStatus() != null) {
                    System.out.println("      Failure Status: " + blobStatus.getFailureStatus());
                }
            }
        }
    }

    private static @NotNull Path resolveQuickstartPath(String fileName) {
        Path preferred = Paths.get("quickstart", fileName);
        if (Files.exists(preferred)) {
            return preferred;
        }
        return Paths.get(fileName);
    }

    private static void closeQuietly(InputStream closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            System.err.println("Failed to close resource: " + e.getMessage());
        }
    }

    private static Path locateConfigFile() {
        List<Path> candidates = new ArrayList<>();
        String override = System.getenv(CONFIG_ENV_OVERRIDE);
        if (StringUtils.isNotBlank(override)) {
            candidates.add(Paths.get(override));
        }
        Path relative = Paths.get(configFileName);
        candidates.add(relative);
        candidates.add(Paths.get(System.getProperty("user.dir", ".")).resolve(relative));
        try {
            Path jarLocation = Paths.get(SampleApp.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            Path jarDir = jarLocation.getParent();
            if (jarDir != null) {
                candidates.add(jarDir.resolve(relative));
                Path parent = jarDir.getParent();
                if (parent != null) {
                    candidates.add(parent.resolve(relative));
                }
            }
        } catch (URISyntaxException ignored) {
            // Fall through to default locations
        }

        return candidates.stream()
                .map(Path::normalize)
                .map(SampleApp::expandWithWorkingDirectory)
                .filter(Files::exists)
                .findFirst()
                .orElseGet(() -> {
                    Utils.errorHandler(String.format(
                            "Couldn't find config file '%s'. Provide it next to the jar, pass an absolute path, or set %s.",
                            configFileName,
                            CONFIG_ENV_OVERRIDE));
                    return relative;
                });
    }

    private static Path expandWithWorkingDirectory(Path path) {
        if (Files.exists(path)) {
            return path;
        }
        Path justFileName = Paths.get(path.getFileName().toString());
        return Files.exists(justFileName) ? justFileName : path;
    }
}
