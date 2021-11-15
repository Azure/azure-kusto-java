// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.quickstart;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.*;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.microsoft.azure.kusto.data.ClientRequestProperties.OPTION_SERVER_TIMEOUT;

public class KustoSampleApp {
    // TODO - Config:
    //  If this quickstart app was downloaded from OneClick, kusto-sample-config.json should be pre-populated with your cluster's details
    //  If this quickstart app was downloaded from GitHub, edit kusto-sample-config.json and modify the cluster URL and database fields appropriately
    private static final String CONFIG_FILE_NAME = "kusto-sample-config.json";

    // TODO - Config (Optional): Change the authentication method from User Prompt to any of the other options
    //  Some of the auth modes require additional environment variables to be set in order to work (see usage in generateConnectionString below)
    //  Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
    private static final String AUTHENTICATION_MODE = "UserPrompt"; // Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)

    // TODO - Config (Optional): Toggle to false to execute this script "unattended"
    private static final boolean WAIT_FOR_USER = true;

    private static String databaseName;
    private static String tableName;
    private static String kustoUrl;
    private static String ingestUrl;
    private static boolean useExistingTable;
    private static List<Map<String, String>> dataToIngest;
    private static String tableSchema;
    private static int step = 1;
    private static final String BATCHING_POLICY = "{ \"MaximumBatchingTimeSpan\": \"00:00:10\", \"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024 }";
    private static final int WAIT_FOR_INGEST_SECONDS = 20;

    public enum ExecutionType {
        CONTROL_COMMAND("Control Command"),
        QUERY("Query");

        private final String name;

        ExecutionType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Kusto sample app is starting...");

        loadConfigs();

        if (AUTHENTICATION_MODE.equals("UserPrompt")) {
            waitForUserToProceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.");
        }

        ConnectionStringBuilder kustoCsb = generateConnectionString(kustoUrl, AUTHENTICATION_MODE);
        ConnectionStringBuilder ingestCsb = generateConnectionString(ingestUrl, AUTHENTICATION_MODE);
        Client kustoClient = null;
        try (IngestClient ingestClient = IngestClientFactory.createClient(ingestCsb)) {
            // Tip: Avoid creating a new Kusto Client for each use. Instead, create the clients once and reuse them.
            kustoClient = new ClientImpl(kustoCsb);

            if (!useExistingTable)
            {
                waitForUserToProceed(String.format("Create table '%s.%s' if it doesn't exist", databaseName, tableName));
                // Tip: This is generally a one-time configuration
                // Learn More: For more information about creating tables, see: https://docs.microsoft.com/azure/data-explorer/one-click-table
                String command = String.format(".create table %s %s", tableName, tableSchema);
                if (!execute(kustoClient, databaseName, command, ExecutionType.CONTROL_COMMAND)) {
                    die("Failed to create table or validate it exists");
                }
            }

            /*
                Learn More:
                Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
                 1) More than 1,000 files were queued for ingestion for the same table by the same user
                 2) More than 1GB of data was queued for ingestion for the same table by the same user
                 3) More than 5 minutes have passed since the first file was queued for ingestion for the same table by the same user
                For more information about customizing the ingestion batching policy, see: https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy

                Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during development and
                 in this sample app, we opt to modify the default ingestion policy to ingest data after 10 seconds have passed.
                Tip 2: This is generally a one-time configuration.
                Tip 3: You can also skip the batching for some files using the Flush-Immediately property, though this option should be used with care as it is inefficient.
             */
            waitForUserToProceed(String.format("Alter the batching policy for '%s.%s'", databaseName, tableName));
            String command = String.format(".alter table %s policy ingestionbatching @'%s'", tableName, BATCHING_POLICY);
            if (!execute(kustoClient, databaseName, command, ExecutionType.CONTROL_COMMAND)) {
                System.out.println("Failed to alter the ingestion policy, which could be the result of insufficient permissions. The sample will still run, though ingestion will be delayed for up to 5 minutes.");
            }

            // Learn More: For more information about Kusto Query Language (KQL), see: https://docs.microsoft.com/azure/data-explorer/write-queries
            waitForUserToProceed(String.format("Get existing row count in '%s.%s'", databaseName, tableName));
            execute(kustoClient, databaseName, String.format("%s | count", tableName), ExecutionType.QUERY);

            for (Map<String, String> file : dataToIngest) {
                IngestionProperties.DataFormat dataFormat = IngestionProperties.DataFormat.valueOf(file.get("format").toLowerCase());
                boolean useExistingMapping = Boolean.parseBoolean(file.get("useExistingMapping"));
                String mappingName = file.get("mappingName");

                // Learn More: For more information about providing inline mappings and mapping references, see: https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings
                if (!useExistingMapping) {
                    String mappingValue = file.get("mappingValue");
                    if (dataFormat.isMappingRequired() && StringUtils.isBlank(mappingValue)) {
                        System.out.printf("The data format %s requires a mapping, but configuration indicates to not use an existing mapping and no mapping was provided. Skipping this ingestion.%n", dataFormat.name());
                        continue;
                    }

                    if (StringUtils.isNotBlank(mappingValue)) {
                        // Tip: This is generally a one-time configuration
                        IngestionMapping.IngestionMappingKind ingestionMappingKind = dataFormat.getIngestionMappingKind();
                        waitForUserToProceed(String.format("Create a %s mapping reference named '%s'", ingestionMappingKind, mappingName));

                        if (StringUtils.isBlank(mappingName)) {
                            mappingName = "DefaultQuickstartMapping" + UUID.randomUUID().toString().substring(0, 5);
                        }
                        String mappingCommand = String.format(".create-or-alter table %s ingestion %s mapping '%s' '%s'", tableName, ingestionMappingKind.name().toLowerCase(), mappingName, mappingValue);
                        if (!execute(kustoClient, databaseName, mappingCommand, ExecutionType.CONTROL_COMMAND)) {
                            System.out.printf("Failed to create a %s mapping reference named '%s'. Skipping this ingestion.%n", ingestionMappingKind, mappingName);
                            continue;
                        }
                    }
                } else if (StringUtils.isBlank(mappingName)) {
                    System.out.println("The configuration indicates an existing mapping should be used, but none was provided. Skipping this ingestion.");
                    continue;
                }

                // Learn More: For more information about ingesting data to Kusto in Java, see: https://docs.microsoft.com/azure/data-explorer/java-ingest-data
                String sourceType = file.get("sourceType");
                String uri = file.get("dataSourceUri");
                waitForUserToProceed(String.format("Ingest '%s' from %s", uri, sourceType));
                // Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even if the file only contains one line.
                //  If the json contains whitespace formatting, use SINGLEJSON. In this case, only one data row json object is allowed per file.
                if (dataFormat == IngestionProperties.DataFormat.json) {
                    dataFormat = IngestionProperties.DataFormat.multijson;
                }

                // Tip: Avoid creating a new ingest Client for each use. Instead, create the client once and use it as long as possible.
                if (sourceType.equalsIgnoreCase("localfilesource")) {
                    ingestDataFromFile(ingestClient, databaseName, tableName, uri, dataFormat, mappingName);
                } else if (sourceType.equalsIgnoreCase("blobsource")) {
                    ingestDataFromBlob(ingestClient, databaseName, tableName, uri, dataFormat, mappingName);
                } else {
                    System.out.printf("Unknown source '%s' for file '%s'%n", sourceType, uri);
                }
            }
        } catch (URISyntaxException | IOException e) {
            die("Couldn't create Kusto client", e);
        }

        System.out.printf("Sleeping %s seconds for queued ingestion to complete. Note: This may take longer depending on the file size and ingestion policy.%n", WAIT_FOR_INGEST_SECONDS);
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

        System.out.printf("Step %s: Get post-ingestion row count for '%s.%s'%n", step++, databaseName, tableName);
        execute(kustoClient, databaseName, String.format("%s | count", tableName), ExecutionType.QUERY);
        System.out.println();

        System.out.printf("Step %s: Get sample of post-ingestion data:%n", step++);
        execute(kustoClient, databaseName, String.format("%s | take 2", tableName), ExecutionType.QUERY);
        System.out.println();
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

    private static void loadConfigs() {
        File configFile = new File(".\\" + KustoSampleApp.CONFIG_FILE_NAME);
        Map<String, Object> configs = null;
        try {
            configs = new ObjectMapper().readValue(configFile, HashMap.class);

            tableName = (String) configs.get("tableName");
            databaseName = (String) configs.get("databaseName");
            kustoUrl = (String) configs.get("kustoUri");
            ingestUrl = (String) configs.get("ingestUri");
            dataToIngest = (List<Map<String, String>>) configs.get("data");
            if (StringUtils.isBlank(tableName) || StringUtils.isBlank(databaseName) || StringUtils.isBlank(kustoUrl)
                    || StringUtils.isBlank(ingestUrl) || StringUtils.isBlank((String) configs.get("useExistingTable"))
                    || dataToIngest.isEmpty()) {
                die(String.format("%s is missing required fields", CONFIG_FILE_NAME));
            }
            useExistingTable = Boolean.parseBoolean((String) configs.get("useExistingTable"));
            tableSchema = (String) configs.get("tableSchema"); // Optional
        } catch (IOException e) {
            die("Couldn't read config file", e);
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

    private static ConnectionStringBuilder generateConnectionString(String endpointUrl, String authenticationMode) {
        ConnectionStringBuilder csb = null;
        switch (authenticationMode) {
            case "UserPrompt":
                csb = ConnectionStringBuilder.createWithUserPrompt(endpointUrl);
                break;
            case "ManagedIdentity":
                // Connect using the system or user provided managed identity (Azure service only)
                // TODO - Config (Optional): Managed identity client id if you are using a User Assigned Managed Id
                String clientId = System.getenv("MANAGED_IDENTITY_CLIENT_ID");
                if (StringUtils.isBlank(clientId)) {
                    csb = ConnectionStringBuilder.createWithAadManagedIdentity(endpointUrl);
                } else {
                    csb = ConnectionStringBuilder.createWithAadManagedIdentity(endpointUrl, clientId);
                }
                break;
            case "AppKey":
                // TODO - Config (Optional): App Id & tenant, and App Key to authenticate with
                // For information about how to procure an AAD Application see: https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
                csb = ConnectionStringBuilder.createWithAadApplicationCredentials(endpointUrl, System.getenv("APP_ID"), System.getenv("APP_KEY"), System.getenv("APP_TENANT"));
                break;
            case "AppCertificate":
                csb = createAppCertificateStringBuilder(endpointUrl);
                break;
            default:
                die(String.format("Authentication mode %s is not supported", authenticationMode));
        }

        csb.setApplicationNameForTracing("KustoJavaSdkQuickStart");
        return csb;
    }

    private static ConnectionStringBuilder createAppCertificateStringBuilder(String endpointUrl) {
        String appId = System.getenv("APP_ID");
        String tenantId = System.getenv("APP_TENANT");
        String privateKeyFilePath = System.getenv("PRIVATE_KEY_PEM_FILE_PATH");
        String publicCertFilePath = System.getenv("PUBLIC_CERT_FILE_PATH");

        try {
            PrivateKey privateKey = SecurityUtils.getPrivateKey(publicCertFilePath);
            X509Certificate x509Certificate = SecurityUtils.getPublicCertificate(privateKeyFilePath);
            return ConnectionStringBuilder.createWithAadApplicationCertificate(endpointUrl, appId, x509Certificate, privateKey, tenantId);
        } catch (IOException | GeneralSecurityException e) {
            die("Couldn't create ConnectionStringBuilder for app certificate authentication", e);
            return null;
        }
    }

    private static boolean execute(Client kustoClient, String databaseName, String query, ExecutionType executionType) {
        ClientRequestProperties clientRequestProperties = createClientRequestProperties("SampleApp_" + executionType);
        KustoOperationResult result;
        try {
            result = kustoClient.execute(databaseName, query, clientRequestProperties);
            System.out.printf("Response from executed %s '%s':%n", executionType.getName(), query);
            KustoResultSetTable primaryResults = result.getPrimaryResults();
            for (int rowNum = 1; primaryResults.next(); rowNum++) {
                KustoResultColumn[] columns = primaryResults.getColumns();
                List<Object> currentRow = primaryResults.getCurrentRow();
                System.out.printf("Record %s%n", rowNum);
                for (int j = 0; j < currentRow.size(); j++) {
                    Object cell = currentRow.get(j);
                    System.out.printf("Column: '%s' of type '%s', Value: '%s'%n", columns[j].getColumnName(), columns[j].getColumnType(), cell == null ? "[null]" : cell);
                }
                System.out.println();
            }
            return true;
        } catch (DataServiceException e) {
            System.out.printf("Server error while trying to execute %s '%s' on database '%s'%n%n", executionType.getName(), query, databaseName);
            e.printStackTrace();
        } catch (DataClientException e) {
            System.out.printf("Client error while trying to execute %s '%s' on database '%s'%n%n", executionType.getName(), query, databaseName);
            e.printStackTrace();
        } catch (Exception e) {
            System.out.printf("Unexpected error while trying to execute %s '%s' on database '%s'%n%n", executionType.getName(), query, databaseName);
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

        // Tip: While not common, you can alter the request default command timeout using the below command, e.g. to set the timeout to 10 minutes, use "10m"
        if (StringUtils.isNotBlank(timeout)) {
            clientRequestProperties.setOption(OPTION_SERVER_TIMEOUT, timeout);
        }
        return clientRequestProperties;
    }

    private static void ingestDataFromFile(IngestClient ingestClient, String databaseName, String tableName, String uri, IngestionProperties.DataFormat dataFormat, String mappingName) {
        IngestionProperties ingestionProperties = createIngestionProperties(databaseName, tableName, dataFormat, mappingName);

        // Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the file descriptor.
        //  Otherwise, the service will determine the file size, but it will require an additional s2s call, and may not be accurate for compressed files.
        // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source id and log it somewhere.

        File fileToIngest = new File(".\\" + uri);
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileToIngest.getPath(), fileToIngest.length(), UUID.randomUUID());
        try {
            ingestClient.ingestFromFile(fileSourceInfo, ingestionProperties);
        } catch (IngestionClientException e) {
            System.out.printf("Client exception while trying to ingest '%s' into '%s.%s'%n%n", uri, databaseName, tableName);
            e.printStackTrace();
        } catch (IngestionServiceException e) {
            System.out.printf("Service exception while trying to ingest '%s' into '%s.%s'%n%n", uri, databaseName, tableName);
            e.printStackTrace();
        }

        // Tip: Kusto's Java SDK can ingest data from files, blobs, java.sql.ResultSet objects, and open streams.
        //  See the SDK's kusto-samples module and the E2E tests in kusto-ingest for additional references.
    }

    private static void ingestDataFromBlob(IngestClient ingestClient, String databaseName, String tableName, String blobUrl, IngestionProperties.DataFormat dataFormat, String mappingName) {
        IngestionProperties ingestionProperties = createIngestionProperties(databaseName, tableName, dataFormat, mappingName);

        // Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the file descriptor instead of the default below of 0.
        //  Otherwise, the service will determine the file size, but it will require an additional s2s call, and may not be accurate for compressed files.
        // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source id and log it somewhere
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

        // Tip: Kusto can ingest data from files, blobs, java.sql.ResultSet objects, and open streams.
        //  See the SDK's kusto-samples module and E2E tests in kusto-ingest for additional references.
    }

    @NotNull
    private static IngestionProperties createIngestionProperties(String databaseName, String tableName, IngestionProperties.DataFormat dataFormat, String mappingName) {
        IngestionProperties ingestionProperties = new IngestionProperties(databaseName, tableName);
        ingestionProperties.setDataFormat(dataFormat);
        // Learn More: For more information about supported data formats, see: https://docs.microsoft.com/azure/data-explorer/ingestion-supported-formats
        if (StringUtils.isNotBlank(mappingName) && dataFormat != null) {
            ingestionProperties.setIngestionMapping(mappingName, dataFormat.getIngestionMappingKind());
        }
        // TODO - Config: Setting the ingestion batching policy takes up to 5 minutes to take effect.
        //  We therefore set Flush-Immediately for the sake of the sample, but it generally shouldn't be used in practice.
        //  Comment out the line below after running the sample the first few times.
        ingestionProperties.setFlushImmediately(true);
        return ingestionProperties;
    }
}