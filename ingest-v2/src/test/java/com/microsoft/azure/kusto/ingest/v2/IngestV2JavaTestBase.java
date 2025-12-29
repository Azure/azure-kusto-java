// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.AzureCliCredentialBuilder;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Base class for Java regression tests for ingest-v2.
 * Provides common setup and utilities
 */
public abstract class IngestV2JavaTestBase {
    
    protected final Logger logger;
    protected final TokenCredential tokenProvider;
    protected final String database;
    protected final String dmEndpoint;
    protected final String engineEndpoint;
    protected final String targetTable;
    protected final Map<String, String> columnNamesToTypes;
    protected Client adminClusterClient;

    public IngestV2JavaTestBase(Class<?> testClass) {
        this.logger = LoggerFactory.getLogger(testClass);
        this.tokenProvider = new AzureCliCredentialBuilder().build();
        
        // Get configuration from environment variables
        this.database = System.getenv("TEST_DATABASE") != null 
            ? System.getenv("TEST_DATABASE") 
            : "e2e";
        
        this.dmEndpoint = System.getenv("DM_CONNECTION_STRING");
        if (this.dmEndpoint == null) {
            throw new IllegalArgumentException("DM_CONNECTION_STRING environment variable is not set");
        }
        
        this.engineEndpoint = dmEndpoint.replace("https://ingest-", "https://");
        
        // Generate unique table name for this test run
        this.targetTable = "V2_Java_Tests_Sensor_" + 
            UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        
        // Define table schema
        this.columnNamesToTypes = new LinkedHashMap<>();
        columnNamesToTypes.put("timestamp", "datetime");
        columnNamesToTypes.put("deviceId", "guid");
        columnNamesToTypes.put("messageId", "guid");
        columnNamesToTypes.put("temperature", "real");
        columnNamesToTypes.put("humidity", "real");
        columnNamesToTypes.put("format", "string");
        columnNamesToTypes.put("SourceLocation", "string");
        columnNamesToTypes.put("Type", "string");
    }

    @BeforeEach
    public void createTables() throws Exception {
        // Build create table script
        StringBuilder columnsBuilder = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : columnNamesToTypes.entrySet()) {
            if (!first) {
                columnsBuilder.append(",");
            }
            columnsBuilder.append("['").append(entry.getKey()).append("']:").append(entry.getValue());
            first = false;
        }
        
        String createTableScript = String.format(
            ".create-merge table %s (%s)",
            targetTable,
            columnsBuilder.toString()
        );
        
        // Build mapping reference script
        StringBuilder mappingBuilder = new StringBuilder();
        mappingBuilder.append(".create-or-alter table ").append(targetTable)
            .append(" ingestion json mapping '").append(targetTable).append("_mapping' ```[\n");
        
        first = true;
        for (String col : columnNamesToTypes.keySet()) {
            if (!first) {
                mappingBuilder.append(",\n");
            }
            
            if (col.equals("SourceLocation")) {
                mappingBuilder.append("    {\"column\":\"").append(col)
                    .append("\", \"Properties\":{\"Transform\":\"SourceLocation\"}}");
            } else if (col.equals("Type")) {
                mappingBuilder.append("    {\"column\":\"").append(col)
                    .append("\", \"Properties\":{\"ConstValue\":\"MappingRef\"}}");
            } else {
                mappingBuilder.append("    {\"column\":\"").append(col)
                    .append("\", \"Properties\":{\"Path\":\"$.").append(col).append("\"}}");
            }
            first = false;
        }
        mappingBuilder.append("\n]```");
        
        // Create admin client
        adminClusterClient = ClientFactory.createClient(
            ConnectionStringBuilder.createWithAzureCli(engineEndpoint)
        );
        
        // Execute table creation and mapping
        adminClusterClient.executeMgmt(database, createTableScript);
        adminClusterClient.executeMgmt(database, mappingBuilder.toString());
        clearDatabaseSchemaCache();
        
        logger.info("Created table: {}", targetTable);
    }

    protected void alterTableToEnableStreaming() throws Exception {
        adminClusterClient.executeMgmt(
            database,
            String.format(".alter table %s policy streamingingestion enable", targetTable)
        );
        logger.info("Enabled streaming ingestion for table: {}", targetTable);
    }

    protected void clearDatabaseSchemaCache() throws Exception {
        adminClusterClient.executeMgmt(
            database,
            ".clear database cache streamingingestion schema"
        );
    }

    @AfterEach
    public void dropTables() throws Exception {
        String dropTableScript = String.format(".drop table %s ifexists", targetTable);
        logger.info("Dropping table {}", targetTable);
        adminClusterClient.executeMgmt(database, dropTableScript);
    }

    /**
     * Wait for data to appear in the table and verify the expected count.
     */
    protected void awaitAndQuery(
        String query,
        String queryColumnName,
        long expectedResultsCount,
        boolean isManagementQuery
    ) {
        Awaitility.await()
            .atMost(Duration.of(3, ChronoUnit.MINUTES))
            .pollInterval(Duration.of(5, ChronoUnit.SECONDS))
            .ignoreExceptions()
            .untilAsserted(() -> {
                KustoResultSetTable results = isManagementQuery
                    ? adminClusterClient.executeMgmt(database, query).getPrimaryResults()
                    : adminClusterClient.executeQuery(database, query).getPrimaryResults();
                
                assertTrue(results.next(), "Query should return results");
                long actualResultCount = results.getLong(queryColumnName);
                
                logger.trace(
                    "For query {}, Current result count: {}, waiting for {}",
                    query,
                    actualResultCount,
                    expectedResultsCount
                );
                
                assertNotNull(results, "Query results should not be null");
                assertTrue(
                    actualResultCount >= expectedResultsCount,
                    String.format("Expected %d counts, got %d", expectedResultsCount, actualResultCount)
                );
            });
    }

    /**
     * Overloaded version with default column name and management query flag.
     */
    protected void awaitAndQuery(String query, long expectedResultsCount) {
        awaitAndQuery(query, "count", expectedResultsCount, false);
    }
}
