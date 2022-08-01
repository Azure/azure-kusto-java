package com.microsoft.azure.kusto.quickstart;

import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.SecurityUtils;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Util static class - Handles the communication with the API, and provides generic and simple "plug-n-play" functions to use in different programs.
 */
public class Utils {
    /**
     * Authentication module of Utils - in charge of authenticating the user with the system
     */
    protected static class Authentication {
        /**
         * Generates Kusto Connection String based on given Authentication Mode.
         *
         * @param clusterUrl         Cluster to connect to
         * @param authenticationMode User Authentication Mode, Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)
         * @return A connection string to be used when creating a Client
         */
        protected static ConnectionStringBuilder generateConnectionString(String clusterUrl, AuthenticationModeOptions authenticationMode) {
            // Learn More: For additional information on how to authorize users and apps in Kusto, see:
            // https://docs.microsoft.com/azure/data-explorer/manage-database-permissions
            switch (authenticationMode) {
                case userPrompt:
                    // Prompt user for credentials
                    return ConnectionStringBuilder.createWithUserPrompt(clusterUrl);

                case managedIdentity:
                    // Authenticate using a System-Assigned managed identity provided to an azure service, or using a User-Assigned managed identity.
                    // For more information, see https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview
                    return createManagedIdentityConnectionString(clusterUrl);

                case appKey:
                    // Learn More: For information about how to procure an AAD Application,
                    // see: https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
                    // TODO (config - optional): App ID & tenant, and App Key to authenticate with
                    return ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUrl,
                            System.getenv("APP_ID"),
                            System.getenv("APP_KEY"),
                            System.getenv("APP_TENANT"));

                case appCertificate:
                    // Authenticate using a certificate file.
                    return createApplicationCertificateConnectionString(clusterUrl);

                default:
                    errorHandler(String.format("Authentication mode '%s' is not supported", authenticationMode));
                    return null;
            }
        }

        /**
         * Generates Kusto Connection String based on 'ManagedIdentity' Authentication Mode
         *
         * @param clusterUrl Url of cluster to connect to
         * @return ManagedIdentity Kusto Connection String
         */
        @NotNull
        private static ConnectionStringBuilder createManagedIdentityConnectionString(String clusterUrl) {
            // Connect using the system- or user-assigned managed identity (Azure service only)
            // TODO (config - optional): Managed identity client ID if you are using a user-assigned managed identity
            String clientId = System.getenv("MANAGED_IDENTITY_CLIENT_ID");
            return StringUtils.isBlank(clientId) ? ConnectionStringBuilder.createWithAadManagedIdentity(clusterUrl) :
                    ConnectionStringBuilder.createWithAadManagedIdentity(clusterUrl, clientId);
        }

        /**
         * Generates Kusto Connection String based on 'AppCertificate' Authentication Mode
         *
         * @param clusterUrl Url of cluster to connect to
         * @return AppCertificate Kusto Connection String
         */
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

            } catch (IOException | GeneralSecurityException e) {
                errorHandler("Couldn't create ConnectionStringBuilder for application certificate authentication", e);
                return null;
            }
        }
    }

    /**
     * Queries module of Utils - in charge of querying the data - either with management queries, or data queries
     */
    protected static class Queries {
        private static final String MgmtPrefix = ".";

        /**
         * Creates a fitting ClientRequestProperties object, to be used when executing control commands or queries
         *
         * @param scope Working scope
         * @return ClientRequestProperties object
         */
        private static ClientRequestProperties createClientRequestProperties(String scope) {
            return createClientRequestProperties(scope, null);
        }

        /**
         * Creates a fitting ClientRequestProperties object, to be used when executing control commands or queries
         *
         * @param scope   Working scope
         * @param timeout Requests default timeout
         * @return ClientRequestProperties object
         */
        private static ClientRequestProperties createClientRequestProperties(String scope, String timeout) {
            ClientRequestProperties clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setClientRequestId(String.format("%s;%s", scope, UUID.randomUUID()));

            // Tip: Though uncommon, you can alter the request default command timeout using the below command, e.g. to set the timeout to 10 minutes, use "10m"
            if (StringUtils.isNotBlank(timeout)) {
                clientRequestProperties.setOption(ClientRequestProperties.OPTION_SERVER_TIMEOUT, timeout);
            }
            return clientRequestProperties;
        }

        /**
         * Executes a Command using a premade client
         *
         * @param kustoClient  Premade client to run Commands.
         * @param databaseName DB name
         * @param command      The Command to execute
         */
        protected static void executeCommand(Client kustoClient, String databaseName, String command) {
            ClientRequestProperties clientRequestProperties = command.startsWith(MgmtPrefix) ?
                    createClientRequestProperties("Java_SampleApp_ControlCommand") : createClientRequestProperties("Java_SampleApp_Query");
            KustoOperationResult result;

            try {
                result = kustoClient.execute(databaseName, command, clientRequestProperties);
                System.out.printf("Response from executed command '%s':%n", command);
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

            } catch (DataServiceException e) {
                errorHandler(String.format("Server error while trying to execute query '%s' on database '%s'%n%n", command, databaseName), e);
            } catch (DataClientException e) {
                errorHandler(String.format("Client error while trying to execute query '%s' on database '%s'%n%n", command, databaseName), e);
            } catch (Exception e) {
                errorHandler(String.format("Unexpected error while trying to execute query '%s' on database '%s'%n%n", command, databaseName), e);
            }
        }
    }

    /**
     * Ingestion module of Utils - in charge of ingesting the given data - based on the configuration file.
     */
    protected static class Ingestion {

        /**
         * Creates a fitting KustoIngestionProperties object, to be used when executing ingestion commands
         *
         * @param databaseName DB name
         * @param tableName    Table name
         * @param dataFormat   Given data format
         * @param mappingName  Desired mapping name
         * @return KustoIngestionProperties object
         */
        @NotNull
        protected static IngestionProperties createIngestionProperties(String databaseName, String tableName, IngestionProperties.DataFormat dataFormat,
                                                                       String mappingName) {
            IngestionProperties ingestionProperties = new IngestionProperties(databaseName, tableName);
            ingestionProperties.setDataFormat(dataFormat);
            // Learn More: For more information about supported data formats, see: https://docs.microsoft.com/azure/data-explorer/ingestion-supported-formats
            if (StringUtils.isNotBlank(mappingName)) {
                ingestionProperties.setIngestionMapping(mappingName, dataFormat.getIngestionMappingKind());
            }

            // TODO (config - optional): Setting the ingestion batching policy takes up to 5 minutes to take effect.
            // We therefore set Flush-Immediately for the sake of the sample, but it generally shouldn't be used in practice.
            // Comment out the line below after running the sample the first few times.
            ingestionProperties.setFlushImmediately(true);

            return ingestionProperties;
        }

        /**
         * Ingest Data from a given file path
         *
         * @param ingestClient Client to ingest data
         * @param databaseName DB name
         * @param tableName    Table name
         * @param filePath     File path
         * @param dataFormat   Given data format
         * @param mappingName  Desired mapping name
         */
        protected static void ingestFromFile(IngestClient ingestClient, String databaseName, String tableName, String filePath,
                                             IngestionProperties.DataFormat dataFormat, String mappingName) {
            IngestionProperties ingestionProperties = createIngestionProperties(databaseName, tableName, dataFormat, mappingName);

            // Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the file descriptor (e.g. fileToIngest.length())
            // instead of the default below of 0.
            // Otherwise, the service will determine the file size, requiring an additional s2s call and may not be accurate for compressed files.
            // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere.
            FileSourceInfo fileSourceInfo = new FileSourceInfo(String.format("quickstart/%s", filePath), 0, UUID.randomUUID());

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

        /**
         * Ingest Data from a given file path
         *
         * @param ingestClient Client to ingest data
         * @param databaseName DB name
         * @param tableName    Table name
         * @param blobUrl      Blob Url
         * @param dataFormat   Given data format
         * @param mappingName  Desired mapping name
         */
        protected static void ingestFromBlob(IngestClient ingestClient, String databaseName, String tableName, String blobUrl,
                                             IngestionProperties.DataFormat dataFormat, String mappingName) {
            IngestionProperties ingestionProperties = createIngestionProperties(databaseName, tableName, dataFormat, mappingName);

            // Tip 1: For optimal ingestion batching and performance,specify the uncompressed data size in the file descriptor instead of the default below of 0
            // Otherwise, the service will determine the file size, requiring an additional s2s call and may not be accurate for compressed files.
            // Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere.
            BlobSourceInfo blobSourceInfo = new BlobSourceInfo(String.format("quickstart/%s", blobUrl), 0, UUID.randomUUID());

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

        /**
         * Halts the program for WaitForIngestSeconds, allowing the queued ingestion process to complete.
         *
         * @param waitForIngestSeconds Sleep time to allow for queued ingestion to complete.
         */
        protected static void waitForIngestionToComplete(int waitForIngestSeconds) {
            System.out.printf("Sleeping %s seconds for queued ingestion to complete. Note: This may take longer depending on the file size and " +
                    "ingestion batching policy.%n", waitForIngestSeconds);
            for (int i = waitForIngestSeconds; i >= 0; i--) {
                System.out.printf("%s.", i);
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                    errorHandler("Exception while sleeping waiting for ingestion, with stack trace:", e);
                }
            }
            System.out.println();
            System.out.println();
        }
    }

    /**
     * Error handling function. Will mention the appropriate error message (and the exception itself if exists), and will quit the program.
     *
     * @param error Appropriate error message received from calling function
     */
    protected static void errorHandler(String error) {
        errorHandler(error, null);
    }

    /**
     * Error handling function. Will mention the appropriate error message (and the exception itself if exists), and will quit the program.
     *
     * @param error Appropriate error message received from calling function
     * @param e     Thrown exception
     */
    protected static void errorHandler(String error, Exception e) {
        System.out.println("Script failed with error: " + error);
        if (e != null) {
            System.out.println("Exception:");
            e.printStackTrace();
        }
        System.exit(-1);
    }
}
