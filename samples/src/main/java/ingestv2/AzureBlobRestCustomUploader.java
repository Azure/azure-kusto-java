// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package ingestv2;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.ChainedTokenCredential;
import com.azure.identity.ChainedTokenCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.ingest.v2.IngestClientBase;
import com.microsoft.azure.kusto.ingest.v2.common.models.mapping.IngestionMapping;
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.FileSource;
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource;
import com.microsoft.azure.kusto.ingest.v2.uploader.ICustomUploader;
import com.microsoft.azure.kusto.ingest.v2.uploader.CustomUploaderHelper;
import com.microsoft.azure.kusto.ingest.v2.uploader.IUploader;
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResult;
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResults;
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadErrorCode;
import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Custom uploader implementation using Azure Blob REST API with SAS tokens from DM config.
 * 
 * This demonstrates end-to-end ICustomUploader usage:
 * 1. Gets container URLs with SAS tokens from DM cluster's config API
 * 2. Uses standard HTTP PUT to upload blobs (same approach works for S3/GCS)
 * 3. Returns BlobSource with the uploaded blob URL for ingestion
 * 
 * ICustomUploader pattern works end-to-end and can be adapted for:
 * - AWS S3 (use AWS SDK or S3 REST API)
 * - Google Cloud Storage (use GCS SDK or REST API)
 * - Any HTTP-based blob storage
 * 
 * <h3>Java Interoperability</h3>
 * 
 * The IUploader interface (obtained via {@code CustomUploaderHelper.asUploader()})
 * provides Java-friendly methods that return {@link java.util.concurrent.CompletableFuture}:
 * <ul>
 *   <li>{@code uploadAsyncJava(LocalSource)} &rarr; {@code CompletableFuture<BlobSource>}</li>
 *   <li>{@code uploadManyAsyncJava(List<LocalSource>)} &rarr; {@code CompletableFuture<UploadResults>}</li>
 * </ul>
 * 
 * This allows Java users to program to the {@code IUploader} interface directly
 * without needing Kotlin coroutine support:
 * <pre>{@code
 * ICustomUploader custom = new AzureBlobRestCustomUploader(containerUrl);
 * IUploader uploader = CustomUploaderHelper.asUploader(custom);
 * 
 * // Use Java-friendly methods directly on IUploader:
 * CompletableFuture<BlobSource> result = uploader.uploadAsyncJava(localSource);
 * CompletableFuture<UploadResults> batchResult = uploader.uploadManyAsyncJava(localSources);
 * 
 * // Or pass to a client builder:
 * QueuedIngestClient client = QueuedIngestClientBuilder.create(dmUrl)
 *     .withAuthentication(credential)
 *     .withUploader(uploader, true)
 *     .build();
 * client.ingestAsyncJava(database, table, fileSource, properties).join();
 * }</pre>
 */
public class AzureBlobRestCustomUploader implements ICustomUploader {
    
    private final String containerUrlWithSas;  // Container URL with SAS token from DM config
    private final ExecutorService executor;
    private boolean ignoreSizeLimit = false;
    
    /**
     * Creates a custom uploader using a container URL with SAS token.
     * 
     * The container URL is obtained from DM cluster's config API:
     * - ConfigurationResponse.containerSettings.containers[0].path
     * 
     * @param containerUrlWithSas Full container URL including SAS token
     *        Example: https://account.blob.core.windows.net/container?sv=...&sig=...
     */
    public AzureBlobRestCustomUploader(String containerUrlWithSas) {
        this.containerUrlWithSas = containerUrlWithSas;
        this.executor = Executors.newFixedThreadPool(4);
    }
    
    @Override
    public boolean getIgnoreSizeLimit() {
        return ignoreSizeLimit;
    }
    
    @Override
    public void setIgnoreSizeLimit(boolean value) {
        this.ignoreSizeLimit = value;
    }
    
    @Override
    public CompletableFuture<BlobSource> uploadAsync(LocalSource local) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return uploadBlobUsingRest(local);
            } catch (Exception e) {
                throw new RuntimeException("Upload failed: " + e.getMessage(), e);
            }
        }, executor);
    }
    
    @Override
    public CompletableFuture<UploadResults> uploadManyAsync(List<? extends LocalSource> localSources) {
        return CompletableFuture.supplyAsync(() -> {
            List<UploadResult.Success> successes = new ArrayList<>();
            List<UploadResult.Failure> failures = new ArrayList<>();
            
            for (LocalSource source : localSources) {
                Instant startedAt = Instant.now();
                try {
                    BlobSource result = uploadBlobUsingRest(source);
                    Long size = source.size();
                    successes.add(new UploadResult.Success(
                        source.getName(),
                        startedAt,
                        Instant.now(),
                        result.getBlobPath(),
                        size != null ? size : -1
                    ));
                } catch (Exception e) {
                    failures.add(new UploadResult.Failure(
                        source.getName(),
                        startedAt,
                        Instant.now(),
                        UploadErrorCode.UPLOAD_FAILED,
                        e.getMessage(),
                        e,
                        false
                    ));
                }
            }
            
            return new UploadResults(successes, failures);
        }, executor);
    }
    
    @Override
    public void close() throws IOException {
        executor.shutdown();
    }
    
    /**
     * Uploads a blob using HTTP PUT with SAS token authentication.
     * 
     * This is the core upload logic that can be adapted for any REST/SDK based storage
     */
    private BlobSource uploadBlobUsingRest(LocalSource local) throws Exception {
        String blobName = generateBlobName(local);
        
        // Read data from source using Kotlin's data() method
        byte[] data;
        try (InputStream inputStream = local.data()) {
            data = inputStream.readAllBytes();
        }
        
        // Parse container URL and SAS
        String[] parts = containerUrlWithSas.split("\\?", 2);
        String containerUrl = parts[0];
        String sasToken = parts.length > 1 ? parts[1] : "";
        
        // Build full blob URL with SAS
        String blobUrl = containerUrl + "/" + blobName;
        if (!sasToken.isEmpty()) {
            blobUrl += "?" + sasToken;
        }
        
        System.out.println("Uploading to: " + containerUrl + "/" + blobName);
        
        // Create HTTP PUT request
        URL url = new URL(blobUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        connection.setConnectTimeout(30000);
        connection.setReadTimeout(60000);
        
        // Set required headers for Azure Blob REST API
        connection.setRequestProperty("x-ms-blob-type", "BlockBlob");
        connection.setRequestProperty("x-ms-version", "2021-06-08");
        connection.setRequestProperty("Content-Type", getContentType(local.getFormat()));
        connection.setRequestProperty("Content-Length", String.valueOf(data.length));
        
        // Upload the data
        try (OutputStream out = connection.getOutputStream()) {
            out.write(data);
            out.flush();
        }
        
        // Check response
        int responseCode = connection.getResponseCode();
        if (responseCode < 200 || responseCode >= 300) {
            String errorBody = "";
            try (InputStream errorStream = connection.getErrorStream()) {
                if (errorStream != null) {
                    errorBody = new String(errorStream.readAllBytes(), StandardCharsets.UTF_8);
                }
            } catch (Exception ignored) {}
            throw new RuntimeException(
                "Upload failed with status " + responseCode + ": " + errorBody
            );
        }
        
        System.out.println("Successfully uploaded blob: " + blobName + " (Status: " + responseCode + ", Size: " + data.length + " bytes)");
        
        // Return BlobSource with the blob URL (including SAS for Kusto to access)
        String resultBlobUrl = containerUrl + "/" + blobName;
        if (!sasToken.isEmpty()) {
            resultBlobUrl += "?" + sasToken;
        }
        
        return new BlobSource(
            resultBlobUrl,
            local.getFormat(),
            local.getSourceId(),
            local.getCompressionType()
        );
    }
    
    private String generateBlobName(LocalSource source) {
        String name = source.getName();
        if (name == null || name.isEmpty()) {
            name = UUID.randomUUID().toString();
        }
        String extension = getExtension(source.getFormat(), source.getCompressionType());
        if (!name.endsWith(extension)) {
            name = name + extension;
        }
        return "custom-upload/" + UUID.randomUUID().toString().substring(0, 8) + "_" + name;
    }
    
    private String getExtension(Format format, CompressionType compression) {
        String formatExt;
        switch (format) {
            case json: formatExt = ".json"; break;
            case csv: formatExt = ".csv"; break;
            case parquet: formatExt = ".parquet"; break;
            case avro: formatExt = ".avro"; break;
            case orc: formatExt = ".orc"; break;
            default: formatExt = ".dat"; break;
        }
        
        if (compression == CompressionType.GZIP) {
            return formatExt + ".gz";
        }
        return formatExt;
    }
    
    private String getContentType(Format format) {
        switch (format) {
            case json: return "application/json";
            case csv: return "text/csv";
            default: return "application/octet-stream";
        }
    }
    
    // ==========================================
    // End-to-end demo using system properties (like other samples)
    // ==========================================
    
    public static void main(String[] args) throws Exception {
        // Get configuration from system properties (consistent with other samples)
        String engineEndpoint = System.getProperty("clusterPath"); // "https://<cluster>.kusto.windows.net"
        String appId = System.getProperty("app-id");
        String appKey = System.getProperty("appKey");
        String tenant = System.getProperty("tenant");
        
        String database = System.getProperty("dbName");
        String table = System.getProperty("tableName");
        String mapping = System.getProperty("dataMappingName");
        
        // Container URL with SAS token (in production, get from DM config API)
        String containerUrlWithSas = System.getProperty("containerUrl");
        
        if (engineEndpoint == null || engineEndpoint.isEmpty()) {
            System.out.println("=== Azure Blob REST Custom Uploader Demo ===");
            System.out.println();
            System.out.println("This demonstrates ICustomUploader end-to-end with Azure Blob REST API.");
            System.out.println();
            System.out.println("Usage:");
            System.out.println("  mvn exec:java -pl samples -Dexec.mainClass=\"ingestv2.AzureBlobRestCustomUploader\" \\");
            System.out.println("    -DclusterPath=https://mycluster.region.kusto.windows.net \\");
            System.out.println("    -DdbName=MyDatabase \\");
            System.out.println("    -DtableName=MyTable \\");
            System.out.println("    -DdataMappingName=MyMapping \\");
            System.out.println("    -DcontainerUrl=https://account.blob.core.windows.net/container?sas=...");
            System.out.println();
            System.out.println("Optional authentication (defaults to Azure CLI credential):");
            System.out.println("    -Dapp-id=<client-id> -DappKey=<secret> -Dtenant=<tenant-id>");
            System.out.println();
            System.out.println("Usage pattern:");
            System.out.println("```java");
            System.out.println("// 1. Get container with SAS from DM config API or provide your own");
            System.out.println("String containerUrlWithSas = ...; // e.g., from ConfigurationResponse");
            System.out.println();
            System.out.println("// 2. Create custom uploader");
            System.out.println("ICustomUploader customUploader = new AzureBlobRestCustomUploader(containerUrlWithSas);");
            System.out.println();
            System.out.println("// 3. Convert to IUploader using CustomUploaderHelper");
            System.out.println("IUploader uploader = CustomUploaderHelper.asUploader(customUploader);");
            System.out.println();
            System.out.println("// 3a. (Optional) Use IUploader Java-friendly methods directly:");
            System.out.println("//   CompletableFuture<BlobSource> result = uploader.uploadAsyncJava(localSource);");
            System.out.println("//   CompletableFuture<UploadResults> batch = uploader.uploadManyAsyncJava(sources);");
            System.out.println();
            System.out.println("// 4. Create QueuedIngestClient with the custom uploader");
            System.out.println("QueuedIngestClient client = QueuedIngestClientBuilder.create(dmUrl)");
            System.out.println("    .withAuthentication(credential)");
            System.out.println("    .withUploader(uploader, true)  // true = client manages uploader lifecycle");
            System.out.println("    .build();");
            System.out.println();
            System.out.println("// 5. Ingest - the custom uploader handles the upload!");
            System.out.println("client.ingestAsyncJava(fileSource, properties).join();");
            System.out.println("```");
            return;
        }
        
        System.out.println("=== Running End-to-End Azure Blob REST Custom Uploader Test ===");
        System.out.println("Engine Endpoint: " + engineEndpoint);
        System.out.println("Database: " + database);
        System.out.println("Table: " + table);
        System.out.println("Mapping: " + mapping);
        System.out.println("Container URL: " + (containerUrlWithSas != null ? "[provided]" : "[not provided - using default uploader]"));
        
        // Create Azure AD credential
        ChainedTokenCredential credential;
        if (StringUtils.isNotBlank(appId)
                && StringUtils.isNotBlank(appKey)
                && StringUtils.isNotBlank(tenant)) {
            System.out.println("Using Service Principal authentication");
            credential = new ChainedTokenCredentialBuilder()
                    .addFirst(new ClientSecretCredentialBuilder()
                            .clientId(appId)
                            .clientSecret(appKey)
                            .tenantId(tenant)
                            .build())
                    .build();
        } else {
            System.out.println("Using Azure CLI authentication");
            credential = new ChainedTokenCredentialBuilder()
                    .addFirst(new AzureCliCredentialBuilder().build())
                    .build();
        }
        
        // Build the ingest URL (DM endpoint)
        String dmUrl = IngestClientBase.getIngestionEndpoint(engineEndpoint);
        if (dmUrl == null) {
            dmUrl = engineEndpoint; // Fallback if transformation not applicable
        }
        System.out.println("DM Endpoint: " + dmUrl);
        
        QueuedIngestClient queuedIngestClient = null;
        try {
            // Create the QueuedIngestClient with or without custom uploader
            QueuedIngestClientBuilder builder = QueuedIngestClientBuilder.create(dmUrl)
                    .withAuthentication(credential)
                    .withMaxConcurrency(10);
            
            // If container URL is not provided, fetch it from the cluster's configuration API
            if (containerUrlWithSas == null || containerUrlWithSas.isEmpty()) {
                System.out.println("\n1. Fetching container URL from Kusto cluster configuration API...");
                containerUrlWithSas = fetchContainerUrlFromKustoConfig(dmUrl, credential);
                System.out.println("   Retrieved container URL from cluster configuration");
            }
            
            // Now create our custom uploader with the container URL
            if (containerUrlWithSas != null && !containerUrlWithSas.isEmpty()) {
                System.out.println("\n2. Creating AzureBlobRestCustomUploader with container URL");
                
                // create the custom uploader
                IUploader uploader = CustomUploaderHelper.asUploader(new AzureBlobRestCustomUploader(containerUrlWithSas));
                
                // Configure the builder to use our custom uploader
                // true = client will manage the uploader lifecycle (close it when done)
                builder.withUploader(uploader, true);
                
                System.out.println("   Custom uploader configured successfully!");
            } else {
                System.out.println("\n2. No container URL available - using default managed uploader");
            }
            
            // Build the client
            queuedIngestClient = builder.build();
            System.out.println("\n3. QueuedIngestClient created successfully");
            
            // Demonstrate ingestion using a file source
            String resourcesDirectory = System.getProperty("user.dir") + "/samples/src/main/resources/";
            
            // Try CSV file first
            FileSource fileSource = new FileSource(Paths.get(resourcesDirectory + "dataset.csv"), Format.csv);

            IngestionMapping ingestionMapping = new IngestionMapping(mapping, IngestionMapping.IngestionMappingType.CSV);

            IngestRequestProperties properties;
            if (mapping != null && !mapping.isEmpty()) {
                properties = IngestRequestPropertiesBuilder.create()
                        .withIngestionMapping(ingestionMapping)
                        .withEnableTracking(true)
                        .build();
            } else {
                properties = IngestRequestPropertiesBuilder.create()
                        .withEnableTracking(true)
                        .build();
            }
            
            System.out.println("\n4. Ingesting file: " + resourcesDirectory + "dataset.csv");
            System.out.println("   (This will use the custom uploader to upload to Azure Blob Storage!)");
            
            // Perform ingestion - the custom uploader handles the upload!
            var response = queuedIngestClient.ingestAsyncJava(database, table, fileSource, properties).get();
            
            System.out.println("\n5. Ingestion queued successfully!");
            System.out.println("   Operation ID: " + response.getIngestResponse().getIngestionOperationId());
            System.out.println("   Ingestion Type: " + response.getIngestionType());

            IngestionMapping jsonMapping = new IngestionMapping(mapping, IngestionMapping.IngestionMappingType.JSON);
            // Also demonstrate JSON ingestion with mapping
            if (mapping != null && !mapping.isEmpty()) {
                FileSource jsonFileSource = new FileSource(Paths.get(resourcesDirectory + "dataset.json"), Format.json);
                
                IngestRequestProperties jsonProperties = IngestRequestPropertiesBuilder.create()
                        .withIngestionMapping(jsonMapping)
                        .withEnableTracking(true)
                        .build();
                
                System.out.println("\n6. Ingesting JSON file with mapping: " + resourcesDirectory + "dataset.json");
                
                var jsonResponse = queuedIngestClient.ingestAsyncJava(database, table, jsonFileSource, jsonProperties).get();
                
                System.out.println("   JSON Ingestion queued successfully!");
                System.out.println("   Operation ID: " + jsonResponse.getIngestResponse().getIngestionOperationId());
            }
            
            // Demonstrate stream ingestion
            String csvData = "0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,\"Zero\",0,00:00:00,,null";
            InputStream csvInputStream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(csvData).array());
            
            StreamSource streamSource = new StreamSource(csvInputStream, Format.csv);
            
            IngestRequestProperties streamProperties = IngestRequestPropertiesBuilder.create()
                    .withEnableTracking(true)
                    .build();
            
            System.out.println("\n7. Ingesting from stream (CSV data)");
            
            var streamResponse = queuedIngestClient.ingestAsyncJava(database, table,streamSource, streamProperties).get();
            
            System.out.println("   Stream Ingestion queued successfully!");
            System.out.println("   Operation ID: " + streamResponse.getIngestResponse().getIngestionOperationId());
            
            System.out.println("\n=== Azure Blob REST Custom Uploader Demo Complete ===");
            System.out.println();
            System.out.println("Key Integration Points:");
            System.out.println("  1. AzureBlobRestCustomUploader implements ICustomUploader");
            System.out.println("  2. CustomUploaderHelper.asUploader() converts to IUploader");
            System.out.println("  3. IUploader now has Java-friendly methods: uploadAsyncJava(), uploadManyAsyncJava()");
            System.out.println("  4. QueuedIngestClientBuilder.withUploader() configures the custom uploader");
            System.out.println("  5. client.ingestAsyncJava() internally uses the custom uploader!");
            System.out.println();
            System.out.println("The same pattern works for any other source such as S3/GCP - just implement ICustomUploader!");
            System.out.println("Java users can also program to the IUploader interface directly using uploadAsyncJava()/uploadManyAsyncJava().");
            
        } catch (Exception e) {
            System.err.println("Error during ingestion: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (queuedIngestClient != null) {
                queuedIngestClient.close();
            }
        }
    }
    
    /**
     * Fetches container URL with SAS token from Kusto cluster's configuration API.
     * 
     * The configuration API endpoint: {dmUrl}/v1/rest/ingestion/configuration
     * Returns JSON with containerSettings.containers[0].path containing the container URL with SAS.
     * 
     * @param dmUrl The DM (Data Management) cluster URL
     * @param credential Azure credential for authentication
     * @return Container URL with SAS token, or null if not available
     */
    private static String fetchContainerUrlFromKustoConfig(String dmUrl, TokenCredential credential) throws Exception {
        String configUrl = dmUrl + "/v1/rest/ingestion/configuration";
        System.out.println("   Fetching configuration from: " + configUrl);
        
        // Get access token for the Kusto resource
        String scope = dmUrl + "/.default";
        com.azure.core.credential.AccessToken token = credential.getToken(
            new com.azure.core.credential.TokenRequestContext().addScopes(scope)
        ).block();
        
        if (token == null) {
            throw new RuntimeException("Failed to get access token for " + scope);
        }
        
        // Create HTTP GET request
        URL url = new URL(configUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Authorization", "Bearer " + token.getToken());
        connection.setRequestProperty("Accept", "application/json");
        connection.setConnectTimeout(30000);
        connection.setReadTimeout(60000);
        
        // Read response
        int responseCode = connection.getResponseCode();
        if (responseCode < 200 || responseCode >= 300) {
            String errorBody = "";
            try (InputStream errorStream = connection.getErrorStream()) {
                if (errorStream != null) {
                    errorBody = new String(errorStream.readAllBytes(), StandardCharsets.UTF_8);
                }
            } catch (Exception ignored) {}
            throw new RuntimeException(
                "Failed to get configuration from " + configUrl + " (status " + responseCode + "): " + errorBody
            );
        }
        
        String responseBody;
        try (InputStream inputStream = connection.getInputStream()) {
            responseBody = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
        
        // Parse JSON response to get container URL
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(responseBody);
        
        // Navigate: containerSettings -> containers -> [0] -> path
        JsonNode containerSettings = root.get("containerSettings");
        if (containerSettings == null) {
            System.out.println("   Warning: No containerSettings in configuration response");
            return null;
        }
        
        JsonNode containers = containerSettings.get("containers");
        if (containers == null || !containers.isArray() || containers.isEmpty()) {
            System.out.println("   Warning: No containers in configuration response");
            return null;
        }
        
        JsonNode firstContainer = containers.get(0);
        JsonNode path = firstContainer.get("path");
        if (path == null || path.isNull()) {
            System.out.println("   Warning: Container path is null");
            return null;
        }
        
        String containerUrl = path.asText();
        System.out.println("   Found container: " + containerUrl.split("\\?")[0] + "?...");
        return containerUrl;
    }
}
