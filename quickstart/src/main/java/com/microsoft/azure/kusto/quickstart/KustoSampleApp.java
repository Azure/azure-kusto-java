// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.quickstart;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultColumn;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoClientInvalidConnectionStringException;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.SecurityUtils;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class KustoSampleApp {
    // TODO (config - optional): Change the authentication method from "User Prompt" to any of the other options
    // Some of the auth modes require additional environment variables to be set in order to work (see usage in generateConnectionString below)
    // Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
    private static final String AUTHENTICATION_MODE = "UserPrompt"; // Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate|DeviceCode)

    // TODO (config - optional): Toggle to false to execute this script "unattended"
    private static final boolean WAIT_FOR_USER = true;

    // TODO (config):
    // If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated with your cluster's details
    // If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL and database fields appropriately
    private static final String CONFIG_FILE_NAME = "kusto_sample_config.json";

    private static final String BATCHING_POLICY = "{ \"MaximumBatchingTimeSpan\": \"00:00:10\", \"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024 }";
    private static final int WAIT_FOR_INGEST_SECONDS = 20;

    private static int step = 1;
    private static boolean useExistingTable;
    private static String databaseName;
    private static String tableName;
    private static String tableSchema;
    private static String kustoUrl;
    private static String ingestUrl;
    private static List<Map<String, String>> dataToIngest;
    private static boolean ignoreFirstRecord;
    private static boolean shouldAlterTable;
    private static boolean shouldQueryData;
    private static boolean shouldIngestData;

    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Kusto sample app is starting...");

        loadConfigs(KustoSampleApp.CONFIG_FILE_NAME);

        if (AUTHENTICATION_MODE.equals("UserPrompt")) {
            waitForUserToProceed("You may be prompted more than once for credentials during this script. Please return to the console after authenticating.");
        }

        try (IngestClient ingestClient = IngestClientFactory.createClient(generateConnectionString(ingestUrl, AUTHENTICATION_MODE))) {
            // Tip: Avoid creating a new Kusto/ingest client for each use. Instead, create the clients once and reuse them.
            Client kustoClient = new ClientImpl(generateConnectionString(kustoUrl, AUTHENTICATION_MODE));

            if (useExistingTable) {
                if (shouldAlterTable) {
                    // Tip: Usually table was originally created with a schema appropriate for the data being ingested, so this wouldn't be needed.
                    // Learn More: For more information about altering table schemas, see:
                    // https://docs.microsoft.com/azure/data-explorer/kusto/management/alter-table-command
                    alterMergeExistingTableToProvidedSchema(kustoClient, databaseName, tableName, tableSchema);
                }

                if (shouldQueryData) {
                    // Learn More: For more information about Kusto Query Language (KQL), see: https://docs.microsoft.com/azure/data-explorer/write-queries
                    queryExistingNumberOfRows(kustoClient, databaseName, tableName);
                }
            } else {
                // Tip: This is generally a one-time configuration.
                // Learn More: For more information about creating tables, see: https://docs.microsoft.com/azure/data-explorer/one-click-table
                createNewTable(kustoClient, databaseName, tableName, tableSchema);
            }

            if (shouldIngestData) {
                for (Map<String, String> file : dataToIngest) {
                    IngestionProperties.DataFormat dataFormat = IngestionProperties.DataFormat.valueOf(file.get("format").toUpperCase());
                    String mappingName = file.get("mappingName");

                    // Tip: This is generally a one-time configuration.
                    // Learn More: For more information about providing inline mappings and mapping references, see:
                    // https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings
                    if (!createIngestionMappings(Boolean.parseBoolean(String.valueOf(file.get("useExistingMapping"))), kustoClient, databaseName, tableName,
                            mappingName, file.get("mappingValue"), dataFormat)) {
                        continue;
                    }
                    // Learn More: For more information about ingesting data to Kusto in Java, see:
                    // https://docs.microsoft.com/azure/data-explorer/java-ingest-data
                    ingest(file, dataFormat, ingestClient, databaseName, tableName, mappingName, ignoreFirstRecord);
                }

                /*
                 * Note: We poll here the ingestion's target table because monitoring successful ingestions is expensive and not recommended. Instead, the
                 * recommended ingestion monitoring approach is to monitor failures. Learn more:
                 * https://docs.microsoft.com/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-status#tracking-ingestion-status-kustoqueuedingestclient
                 * and https://docs.microsoft.com/azure/data-explorer/using-diagnostic-logs
                 */
                waitForIngestionToComplete();
            }

            if (shouldQueryData) {
                executeValidationQueries(kustoClient, databaseName, tableName, shouldIngestData);
            }
        } catch (URISyntaxException | KustoClientInvalidConnectionStringException e) {
            die("Couldn't create Kusto client", e);
        }
    }

    private static void waitForUserToProceed(String upcomingOperation) {
        System.out.println();
        System.out.printf("Step %s: %s%n", step++, upcomingOperation);
        if (WAIT_FOR_USER) {
            System.out.println("Press ENTER to proceed with this operation...");
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }
    }

    private static void die(String error) {
        die(error, null);
    }

    private static void die(String error, Exception ex) {
        System.out.println("Script failed with error: " + error);
        if (ex != null) {
            System.out.println("Exception:");
            ex.printStackTrace();
        }
        System.exit(-1);
    }

    private static void loadConfigs(String configFileName) {
        File configFile = new File(".\\" + configFileName);
        Map<String, Object> configs;
        try {
            configs = new ObjectMapper().readValue(configFile, HashMap.class);

            useExistingTable = Boolean.parseBoolean(configs.get("useExistingTable").toString());
            databaseName = (String) configs.get("databaseName");
            tableName = (String) configs.get("tableName");
            tableSchema = (String) configs.get("tableSchema");
            kustoUrl = (String) configs.get("kustoUri");
            ingestUrl = (String) configs.get("ingestUri");
            dataToIngest = (List<Map<String, String>>) configs.get("data");
            ignoreFirstRecord = Boolean.parseBoolean(configs.get("ignoreFirstRecord").toString());
            shouldAlterTable = Boolean.parseBoolean(configs.get("alterTable").toString());
            shouldQueryData = Boolean.parseBoolean(configs.get("queryData").toString());
            shouldIngestData = Boolean.parseBoolean(configs.get("ingestData").toString());
            if (StringUtils.isBlank(databaseName) || StringUtils.isBlank(tableName) || StringUtils.isBlank(tableSchema)
                    || StringUtils.isBlank(kustoUrl) || StringUtils.isBlank(ingestUrl)
                    || StringUtils.isBlank(configs.get("useExistingTable").toString()) || dataToIngest.isEmpty()) {
                die(String.format("File '%s' is missing required fields", configFileName));
            }
        } catch (IOException e) {
            die(String.format("Couldn't read config file from file '%s'", configFileName), e);
        }
    }

    private static ConnectionStringBuilder generateConnectionString(String clusterUrl, String authenticationMode) throws KustoClientInvalidConnectionStringException {
        // Learn More: For additional information on how to authorize users and apps in Kusto, see:
        // https://docs.microsoft.com/azure/data-explorer/manage-database-permissions
        ConnectionStringBuilder csb = null;
        switch (authenticationMode) {
            case "UserPrompt":
                // Prompt user for credentials
                csb = ConnectionStringBuilder.createWithUserPrompt(clusterUrl);
                break;
            case "ManagedIdentity":
                csb = createManagedIdentityConnectionString(clusterUrl);
                break;
            case "AppKey":
                // Learn More: For information about how to procure an AAD Application, see:
                // https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
                // TODO (config - optional): App ID & tenant, and App Key to authenticate with
                csb = ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUrl,
                        System.getenv("APP_ID"),
                        System.getenv("APP_KEY"),
                        System.getenv("APP_TENANT"));
                break;
            case "AppCertificate":
                csb = createApplicationCertificateConnectionString(clusterUrl);
                break;
            case "DeviceCode":
                csb = ConnectionStringBuilder.createWithDeviceCode(clusterUrl);
                break;
            default:
                die(String.format("Authentication mode '%s' is not supported", authenticationMode));
        }

        csb.setApplicationNameForTracing("KustoJavaSdkQuickStart");
        return csb;
    }

    @NotNull
    private static ConnectionStringBuilder createManagedIdentityConnectionString(String clusterUrl) throws KustoClientInvalidConnectionStringException {
        // Connect using the system- or user-assigned managed identity (Azure service only)
        // TODO (config - optional): Managed identity client ID if you are using a user-assigned managed identity
        String clientId = System.getenv("MANAGED_IDENTITY_CLIENT_ID");
        if (StringUtils.isBlank(clientId)) {
            return ConnectionStringBuilder.createWithAadManagedIdentity(clusterUrl);
        } else {
            return ConnectionStringBuilder.createWithAadManagedIdentity(clusterUrl, clientId);
        }
    }

    private static ConnectionStringBuilder createApplicationCertificateConnectionString(String clusterUrl) {
        // TODO (config - optional): App ID & tenant, path to public certificate and path to private certificate pem file to authenticate with
        String appId = System.getenv("APP_ID");
        String appTenant = System.getenv("APP_TENANT");
        String privateKeyPemFilePath = System.getenv("PRIVATE_KEY_PEM_FILE_PATH");
        String publicCertFilePath = System.getenv("PUBLIC_CERT_FILE_PATH");

        try {
            PrivateKey privateKey = SecurityUtils.getPrivateKey(publicCertFilePath);
            X509Certificate x509Certificate = SecurityUtils.getPublicCertificate(privateKeyPemFilePath);
            return ConnectionStringBuilder.createWithAadApplicationCertificate(clusterUrl, appId, x509Certificate, privateKey, appTenant);
        } catch (IOException | GeneralSecurityException | KustoClientInvalidConnectionStringException e) {
            die("Couldn't create ConnectionStringBuilder for application certificate authentication", e);
            return null;
        }
    }

    private static void createNewTable(Client kustoClient, String databaseName, String tableName, String tableSchema) {
        waitForUserToProceed(String.format("Create table '%s.%s'", databaseName, tableName));
        String command = String.format(".create table %s %s", tableName, tableSchema);
        if (!executeControlCommand(kustoClient, databaseName, command)) {
            die(String.format("Failed to create table or validate it exists using command '%s'", command));
        }

        /*
         * Learn More: Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met: 1)
         * More than 1,000 files were queued for ingestion for the same table by the same user 2) More than 1GB of data was queued for ingestion for the same
         * table by the same user 3) More than 5 minutes have passed since the first file was queued for ingestion for the same table by the same user For more
         * information about customizing the ingestion batching policy, see: https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy
         */
        // Disabled to prevent an existing batching policy from being unintentionally changed
        if (false) {
            alterBatchingPolicy(kustoClient, databaseName, tableName);
        }
    }

    private static void alterMergeExistingTableToProvidedSchema(Client kustoClient, String databaseName, String tableName, String tableSchema) {
        waitForUserToProceed(String.format("Alter-merge existing table '%s.%s' to align with the provided schema", databaseName, tableName));
        String command = String.format(".alter-merge table %s %s", tableName, tableSchema);
        if (!executeControlCommand(kustoClient, databaseName, command)) {
            die(String.format("Failed to alter table using command '%s'", command));
        }
    }

    private static boolean executeControlCommand(Client kustoClient, String databaseName, String controlCommand) {
        ClientRequestProperties clientRequestProperties = createClientRequestProperties("Java_SampleApp_ControlCommand");
        KustoOperationResult result;
        try {
            result = kustoClient.execute(databaseName, controlCommand, clientRequestProperties);
            // Tip: Actual implementations wouldn't generally print the response from a control command. We print here to demonstrate what the response looks
            // like.
            System.out.printf("Response from executed control command '%s':%n", controlCommand);
            KustoResultSetTable primaryResults = result.getPrimaryResults();
            for (int rowNum = 1; primaryResults.next(); rowNum++) {
                KustoResultColumn[] columns = primaryResults.getColumns();
                List<Object> currentRow = primaryResults.getCurrentRow();
                System.out.printf("Record %s%n", rowNum);
                for (int j = 0; j < currentRow.size(); j++) {
                    Object cell = currentRow.get(j);
                    System.out.printf("Column: '%s' of type '%s', Value: '%s'%n", columns[j].getColumnName(), columns[j].getColumnType(),
                            cell == null ? "[null]" : cell);
                }
                System.out.println();
            }
            return true;
        } catch (DataServiceException e) {
            System.out.printf("Server error while trying to execute control command '%s' on database '%s'%n%n", controlCommand, databaseName);
            e.printStackTrace();
        } catch (DataClientException e) {
            System.out.printf("Client error while trying to execute control command '%s' on database '%s'%n%n", controlCommand, databaseName);
            e.printStackTrace();
        } catch (Exception e) {
            System.out.printf("Unexpected error while trying to execute control command '%s' on database '%s'%n%n", controlCommand, databaseName);
            e.printStackTrace();
        }
        return false;
    }

    private static boolean executeQuery(Client kustoClient, String databaseName, String query) {
        ClientRequestProperties clientRequestProperties = createClientRequestProperties("Java_SampleApp_Query");
        KustoOperationResult result;
        try {
            result = kustoClient.execute(databaseName, query, clientRequestProperties);
            System.out.printf("Response from executed query '%s':%n", query);
            KustoResultSetTable primaryResults = result.getPrimaryResults();
            for (int rowNum = 1; primaryResults.next(); rowNum++) {
                KustoResultColumn[] columns = primaryResults.getColumns();
                List<Object> currentRow = primaryResults.getCurrentRow();
                System.out.printf("Record %s%n", rowNum);
                for (int j = 0; j < currentRow.size(); j++) {
                    Object cell = currentRow.get(j);
                    System.out.printf("Column: '%s' of type '%s', Value: '%s'%n", columns[j].getColumnName(), columns[j].getColumnType(),
                            cell == null ? "[null]" : cell);
                }
                System.out.println();
            }
            return true;
        } catch (DataServiceException e) {
            System.out.printf("Server error while trying to execute query '%s' on database '%s'%n%n", query, databaseName);
            e.printStackTrace();
        } catch (DataClientException e) {
            System.out.printf("Client error while trying to execute query '%s' on database '%s'%n%n", query, databaseName);
            e.printStackTrace();
        } catch (Exception e) {
            System.out.printf("Unexpected error while trying to execute query '%s' on database '%s'%n%n", query, databaseName);
            e.printStackTrace();
        }
        return false;
    }

    private static ClientRequestProperties createClientRequestProperties(String scope) {
        return createClientRequestProperties(scope, null);
    }

    private static ClientRequestProperties createClientRequestProperties(String scope, String timeout) {
        ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
        clientRequestProperties.setClientRequestId(String.format("%s;%s", scope, UUID.randomUUID()));

        // Tip: Though uncommon, you can alter the request default command timeout using the below command, e.g. to set the timeout to 10 minutes, use "10m".
        if (StringUtils.isNotBlank(timeout)) {
            clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, timeout);
        }
        return clientRequestProperties;
    }

    private static void queryExistingNumberOfRows(Client kustoClient, String databaseName, String tableName) {
        waitForUserToProceed(String.format("Get existing row count in '%s.%s'", databaseName, tableName));
        executeQuery(kustoClient, databaseName, String.format("%s | count", tableName));
    }

    private static void alterBatchingPolicy(Client kustoClient, String databaseName, String tableName) {
        /*
         * Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during development and in this sample app, we opt to modify
         * the default ingestion policy to ingest data after at most 10 seconds. Tip 2: This is generally a one-time configuration. Tip 3: You can also skip the
         * batching for some files using the Flush-Immediately property, though this option should be used with care as it is inefficient.
         */
        waitForUserToProceed(String.format("Alter the batching policy for table '%s.%s'", databaseName, tableName));
        String command = String.format(".alter table %s policy ingestionbatching @'%s'", tableName, BATCHING_POLICY);
        if (!executeControlCommand(kustoClient, databaseName, command)) {
            System.out.println(
                    "Failed to alter the ingestion policy, which could be the result of insufficient permissions. The sample will still run, though ingestion will be delayed for up to 5 minutes.");
        }
    }

    private static boolean createIngestionMappings(boolean useExistingMapping, Client kustoClient, String databaseName, String tableName, String mappingName,
            String mappingValue, IngestionProperties.DataFormat dataFormat) {
        if (!useExistingMapping && StringUtils.isNotBlank(mappingValue)) {
            IngestionMapping.IngestionMappingKind ingestionMappingKind = dataFormat.getIngestionMappingKind();
            waitForUserToProceed(String.format("Create a '%s' mapping reference named '%s'", ingestionMappingKind.getKustoValue(), mappingName));

            if (StringUtils.isBlank(mappingName)) {
                mappingName = "DefaultQuickstartMapping" + UUID.randomUUID().toString().substring(0, 5);
            }
            String mappingCommand = String.format(".create-or-alter table %s ingestion %s mapping '%s' '%s'", tableName,
                    ingestionMappingKind.getKustoValue().toLowerCase(), mappingName, mappingValue);
            if (!executeControlCommand(kustoClient, databaseName, mappingCommand)) {
                System.out.printf("Failed to create a '%s' mapping reference named '%s'. Skipping this ingestion.%n", ingestionMappingKind.getKustoValue(),
                        mappingName);
                return false;
            }
        }
        return true;
    }

    private static void ingest(Map<String, String> file, IngestionProperties.DataFormat dataFormat, IngestClient ingestClient, String databaseName,
            String tableName, String mappingName, boolean ignoreFirstRecord) {
        String sourceType = file.get("sourceType");
        String uri = file.get("dataSourceUri");
        waitForUserToProceed(String.format("Ingest '%s' from '%s'", uri, sourceType));
        // Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even if the file only contains one line.
        // If the json contains whitespace formatting, use SINGLEJSON. In this case, only one data row json object is allowed per file.
        if (dataFormat == IngestionProperties.DataFormat.JSON) {
            dataFormat = IngestionProperties.DataFormat.MULTIJSON;
        }

        // Tip: Kusto's Java SDK can ingest data from files, blobs, java.sql.ResultSet objects, and open streams.
        // See the SDK's kusto-samples module and the E2E tests in kusto-ingest for additional references.
        if (sourceType.equalsIgnoreCase("localfilesource")) {
            ingestFromFile(ingestClient, databaseName, tableName, uri, dataFormat, mappingName, ignoreFirstRecord);
        } else if (sourceType.equalsIgnoreCase("blobsource")) {
            ingestFromBlob(ingestClient, databaseName, tableName, uri, dataFormat, mappingName, ignoreFirstRecord);
        } else {
            System.out.printf("Unknown source '%s' for file '%s'%n", sourceType, uri);
        }
    }

    private static void ingestFromFile(IngestClient ingestClient, String databaseName, String tableName, String filePath,
            IngestionProperties.DataFormat dataFormat, String mappingName, boolean ignoreFirstRecord) {
        IngestionProperties ingestionProperties = createIngestionProperties(databaseName, tableName, dataFormat, mappingName, ignoreFirstRecord);

        // Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the file descriptor (e.g. fileToIngest.length()) instead
        // of the default below of 0.
        // Otherwise, the service will determine the file size, requiring an additional s2s call and may not be accurate for compressed files.
        // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere.
        FileSourceInfo fileSourceInfo = new FileSourceInfo(filePath, 0, UUID.randomUUID());
        try {
            ingestClient.ingestFromFile(fileSourceInfo, ingestionProperties);
        } catch (IngestionClientException e) {
            System.out.printf("Client exception while trying to ingest '%s' into '%s.%s'%n%n", filePath, databaseName, tableName);
            e.printStackTrace();
        } catch (IngestionServiceException e) {
            System.out.printf("Service exception while trying to ingest '%s' into '%s.%s'%n%n", filePath, databaseName, tableName);
            e.printStackTrace();
        }
    }

    private static void ingestFromBlob(IngestClient ingestClient, String databaseName, String tableName, String blobUrl,
            IngestionProperties.DataFormat dataFormat, String mappingName, boolean ignoreFirstRecord) {
        IngestionProperties ingestionProperties = createIngestionProperties(databaseName, tableName, dataFormat, mappingName, ignoreFirstRecord);

        // Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the file descriptor instead of the default below of 0.
        // Otherwise, the service will determine the file size, requiring an additional s2s call and may not be accurate for compressed files.
        // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere.
        BlobSourceInfo blobSourceInfo = new BlobSourceInfo(blobUrl, 0, UUID.randomUUID());
        try {
            ingestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
        } catch (IngestionClientException e) {
            System.out.printf("Client exception while trying to ingest '%s' into '%s.%s'%n%n", blobUrl, databaseName, tableName);
            e.printStackTrace();
        } catch (IngestionServiceException e) {
            System.out.printf("Service exception while trying to ingest '%s' into '%s.%s'%n%n", blobUrl, databaseName, tableName);
            e.printStackTrace();
        }
    }

    @NotNull
    private static IngestionProperties createIngestionProperties(String databaseName, String tableName, IngestionProperties.DataFormat dataFormat,
            String mappingName, boolean ignoreFirstRecord) {
        IngestionProperties ingestionProperties = new IngestionProperties(databaseName, tableName);
        ingestionProperties.setDataFormat(dataFormat);
        // Learn More: For more information about supported data formats, see: https://docs.microsoft.com/azure/data-explorer/ingestion-supported-formats
        if (StringUtils.isNotBlank(mappingName)) {
            ingestionProperties.setIngestionMapping(mappingName, dataFormat.getIngestionMappingKind());
        }
        ingestionProperties.setIgnoreFirstRecord(ignoreFirstRecord);
        // TODO (config - optional): Setting the ingestion batching policy takes up to 5 minutes to take effect.
        // We therefore set Flush-Immediately for the sake of the sample, but it generally shouldn't be used in practice.
        // Comment out the line below after running the sample the first few times.
        ingestionProperties.setFlushImmediately(true);
        return ingestionProperties;
    }

    private static void waitForIngestionToComplete() throws InterruptedException {
        System.out.printf(
                "Sleeping %s seconds for queued ingestion to complete. Note: This may take longer depending on the file size and ingestion batching policy.%n",
                WAIT_FOR_INGEST_SECONDS);
        for (int i = WAIT_FOR_INGEST_SECONDS; i >= 0; i--) {
            System.out.printf("%s.", i);
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                System.out.println("Exception while sleeping waiting for ingestion, with stack trace:");
                e.printStackTrace();
                throw e;
            }
        }
        System.out.println();
        System.out.println();
    }

    private static void executeValidationQueries(Client kustoClient, String databaseName, String tableName, boolean shouldIngestData) {
        String optionalPostIngestionMessage = shouldIngestData ? "post-ingestion " : "";
        System.out.printf("Step %s: Get %srow count for '%s.%s'%n", step++, optionalPostIngestionMessage, databaseName, tableName);
        executeQuery(kustoClient, databaseName, String.format("%s | count", tableName));
        System.out.println();

        System.out.printf("Step %s: Get sample (2 records) of %sdata:%n", step++, optionalPostIngestionMessage);
        executeQuery(kustoClient, databaseName, String.format("%s | take 2", tableName));
        System.out.println();
    }
}
