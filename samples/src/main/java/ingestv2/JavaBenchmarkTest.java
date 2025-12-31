package ingestv2;

import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.ChainedTokenCredential;
import com.azure.identity.ChainedTokenCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder;
import com.microsoft.azure.kusto.ingest.v2.client.IngestionOperation;
import com.microsoft.azure.kusto.ingest.v2.client.QueuedIngestClient;
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse;
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder;
import com.microsoft.azure.kusto.ingest.v2.models.Format;
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties;
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse;
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType;
import com.microsoft.azure.kusto.ingest.v2.source.FileSource;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * JMH Microbenchmark for Kusto Queued Ingestion V2 API
 * This benchmark measures the performance of:
 * 1. Queueing ingestion operations
 * 2. End-to-end ingestion including polling for completion
 * 3. Batch ingestion performance
 * To run the benchmark:
 * 1. Set environment variables:
 *    - DM_CONNECTION_STRING: https://<cluster>.kusto.windows.net
 *    - TEST_DATABASE: database name
 *    - KUSTO_TABLE: table name
 *    - KUSTO_APP_ID (optional): Azure AD app ID
 *    - KUSTO_APP_KEY (optional): Azure AD app key
 *    - KUSTO_TENANT_ID (optional): Azure AD tenant ID
 *    - BLOB_URL: blob URL with SAS token to ingest
 * 2. Build: mvn clean package
 * 3. Run: java -jar target/samples.jar
 * Or run specific benchmark:
 * java -jar target/benchmarks.jar QueuedIngestionBenchmark.benchmarkSingleQueueOperation
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class JavaBenchmarkTest {

    // Configuration from environment variables
    private static final String CLUSTER_URL = System.getenv().getOrDefault("DM_CONNECTION_STRING", "");
    private static final String BLOB_FILE_NAME_PREFIX = "StormEvents_" + UUID.randomUUID() ;
    private static final String DATABASE = System.getenv().getOrDefault("TEST_DATABASE", "e2e");
    private static final String TABLE = System.getenv().getOrDefault("KUSTO_TABLE", BLOB_FILE_NAME_PREFIX);
    private static final String APP_ID = System.getenv().getOrDefault("KUSTO_APP_ID", "");
    private static final String APP_KEY = System.getenv().getOrDefault("KUSTO_APP_KEY", "");
    private static final String TENANT_ID = System.getenv().getOrDefault("KUSTO_TENANT_ID", "");
    private static final String BLOB_URL = System.getenv().getOrDefault("BLOB_URL",
            "https://kustosamples.blob.core.windows.net/samplefiles/StormEvents.csv");
    private static final String STREAMING_JSON_URL = "https://kustosamplefiles.blob.core.windows.net/jsonsamplefiles/multilined.json";
    // create a random file name to download the blob file in the temp directory
    private static final Path TEMP_FILE_PATH;
    private static final Path STREAMING_JSON_TEMP_FILE_PATH;

    static {
        try {
            TEMP_FILE_PATH = Files.createTempFile(BLOB_FILE_NAME_PREFIX, ".csv");
            STREAMING_JSON_TEMP_FILE_PATH = Files.createTempFile("StreamingIngestSample_" + UUID.randomUUID(), ".json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @State(Scope.Benchmark)
    public static class ClientState {
        QueuedIngestClient queuedIngestClient;
        Client kustoClient;
        IngestRequestProperties ingestProperties;
        String testTableName;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            if (CLUSTER_URL.isEmpty() || DATABASE.isEmpty() || TABLE.isEmpty()) {
                throw new IllegalStateException(
                        "Please set DM_CONNECTION_STRING, TEST_DATABASE, and KUSTO_TABLE environment variables");
            }

            // Create credential with fallback to Azure CLI
            ChainedTokenCredential credential;
            boolean useAppCredentials = !APP_ID.isEmpty() && !APP_KEY.isEmpty() && !TENANT_ID.isEmpty();
            if (useAppCredentials) {
                System.out.println("Using ClientSecret credential");
                credential = new ChainedTokenCredentialBuilder()
                        .addFirst(new ClientSecretCredentialBuilder()
                                .clientId(APP_ID)
                                .clientSecret(APP_KEY)
                                .tenantId(TENANT_ID)
                                .build())
                        .addLast(new AzureCliCredentialBuilder().build())
                        .build();
            } else {
                System.out.println("Using Azure CLI credential");
                credential = new ChainedTokenCredentialBuilder()
                        .addFirst(new AzureCliCredentialBuilder().build())
                        .build();
            }

            // Create V2 QueuedIngestClient
            queuedIngestClient = QueuedIngestClientBuilder.create(CLUSTER_URL)
                    .withAuthentication(credential)
                    .withMaxConcurrency(10)
                    .build();

            // Create Kusto client for table management
            ConnectionStringBuilder csb;
            String engineUrl = CLUSTER_URL.replace("https://ingest-", "https://");
            if (useAppCredentials) {
                csb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                        engineUrl, APP_ID, APP_KEY, TENANT_ID);
            } else {
                csb = ConnectionStringBuilder.createWithAzureCli(engineUrl);
            }
            kustoClient = ClientFactory.createClient(csb);

            // Create test table
            testTableName = "StormsBenchmarkTest_" + System.currentTimeMillis();
            String tableColumns = " (StartTime:datetime, EndTime:datetime, EpisodeId:int, EventId:int, State:string, EventType:string, InjuriesDirect:int, InjuriesIndirect:int, DeathsDirect:int, DeathsIndirect:int, DamageProperty:int, DamageCrops:int, Source:string, BeginLocation:string, EndLocation:string, BeginLat:real, BeginLon:real, EndLat:real, EndLon:real, EpisodeNarrative:string, EventNarrative:string, StormSummary:dynamic)";
            kustoClient.executeToJsonResult(DATABASE, ".create table " + testTableName + " " + tableColumns, null);

            // Set auto-delete policy
            LocalDateTime time = LocalDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")).plusHours(1);
            String expiryDate = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(time);
            String autoDeletePolicy = "@'{ \"ExpiryDate\" : \"" + expiryDate + "\", \"DeleteIfNotEmpty\": true }'";
            kustoClient.executeToJsonResult(DATABASE,
                    String.format(".alter table %s policy auto_delete %s", testTableName, autoDeletePolicy), null);
            kustoClient.executeToJsonResult(DATABASE,
                    String.format(".alter database %s policy ingestionbatching " +
                            "@'{\"MaximumBatchingTimeSpan\":\"00:00:10\",\"MaximumNumberOfItems\":100,\"MaximumRawDataSizeMB\":1024}'", DATABASE), null);

            // Download blob file to temp location for FileSource benchmark
            System.out.println("Downloading blob file from: " + BLOB_URL);
            try (InputStream in = new URL(BLOB_URL).openStream()) {
                Files.copy(in, TEMP_FILE_PATH, StandardCopyOption.REPLACE_EXISTING);
                System.out.println("Downloaded blob file to: " + TEMP_FILE_PATH + " (size: " + Files.size(TEMP_FILE_PATH) + " bytes)");
            } catch (IOException e) {
                System.err.println("Failed to download blob file: " + e.getMessage());
                throw new RuntimeException("Failed to download test blob file", e);
            }

            // Download JSON file for streaming ingestion
            System.out.println("Downloading streaming JSON file from: " + STREAMING_JSON_URL);
            try (InputStream in = new URL(STREAMING_JSON_URL).openStream()) {
                Files.copy(in, STREAMING_JSON_TEMP_FILE_PATH, StandardCopyOption.REPLACE_EXISTING);
                System.out.println("Downloaded streaming JSON file to: " + STREAMING_JSON_TEMP_FILE_PATH + " (size: " + Files.size(STREAMING_JSON_TEMP_FILE_PATH) + " bytes)");
            } catch (IOException e) {
                System.err.println("Failed to download streaming JSON file: " + e.getMessage());
                throw new RuntimeException("Failed to download streaming JSON file", e);
            }

            // Create ingestion properties
            ingestProperties = IngestRequestPropertiesBuilder
                    .create(DATABASE, testTableName)
                    .withEnableTracking(true)
                    .build();

            System.out.println("Benchmark setup complete. Test table: " + testTableName);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (kustoClient != null && testTableName != null) {
                try {
                    kustoClient.executeToJsonResult(DATABASE, ".drop table " + testTableName + " ifexists", null);
                    System.out.println("Dropped test table: " + testTableName);
                } catch (Exception e) {
                    System.err.println("Failed to drop table: " + e.getMessage());
                }
                // Client doesn't need explicit closing in this version
            }
            if (queuedIngestClient != null) {
                queuedIngestClient.close();
            }

            // Clean up temp file
            try {
                if (Files.exists(TEMP_FILE_PATH)) {
                    Files.delete(TEMP_FILE_PATH);
                    System.out.println("Deleted temp file: " + TEMP_FILE_PATH);
                }
                if (Files.exists(STREAMING_JSON_TEMP_FILE_PATH)) {
                    Files.delete(STREAMING_JSON_TEMP_FILE_PATH);
                    System.out.println("Deleted streaming JSON temp file: " + STREAMING_JSON_TEMP_FILE_PATH);
                }
            } catch (IOException e) {
                System.err.println("Failed to delete temp file: " + e.getMessage());
            }
        }
    }


    /**
     * Benchmark: End-to-end ingestion with polling
     * Measures complete ingestion cycle including waiting for completion
     */
    @Benchmark
    public void benchmarkEndToEndIngestionWithPolling(@NotNull ClientState state, @NotNull Blackhole blackhole) throws Exception {
        FileSource blobSource = new FileSource(
                TEMP_FILE_PATH,
                Format.csv,
                UUID.randomUUID(),
                CompressionType.GZIP
        );
        // Queue ingestion
        ExtendedIngestResponse response = state.queuedIngestClient.ingestAsync(blobSource, state.ingestProperties).get();
        String opId = response.getIngestResponse().getIngestionOperationId();

        // Create operation for tracking
        assert opId != null;
        IngestionOperation operation = new IngestionOperation(
                opId,
                DATABASE,
                state.testTableName,
                response.getIngestionType()
        );
        // Poll for completion (with 2 minute timeout)
        StatusResponse finalStatus = state.queuedIngestClient.pollForCompletion(
                operation,
                Duration.ofSeconds(5),
                Duration.ofMinutes(2)
        ).get();
        blackhole.consume(finalStatus.getStatus());
    }

    /**
     * Benchmark: Parallel ingestion operations
     * Measures concurrent ingestion performance
     */
    @Benchmark
    @OperationsPerInvocation(5)
    @Threads(5)
    public void benchmarkParallelIngestion(@NotNull ClientState state, @NotNull Blackhole blackhole) throws Exception {
        FileSource blobSource = new FileSource(
                TEMP_FILE_PATH,
                Format.csv,
                UUID.randomUUID(),
                CompressionType.GZIP
        );
        ExtendedIngestResponse response = state.queuedIngestClient.ingestAsync(blobSource, state.ingestProperties).get();
        blackhole.consume(response.getIngestResponse().getIngestionOperationId());
    }

    /**
     * Benchmark: Managed Streaming Ingestion
     * Ingests a JSON file using streaming ingestion with polling for completion
     */
    @Benchmark
    public void benchmarkManagedStreamingIngestion(@NotNull ClientState state, @NotNull Blackhole blackhole) throws Exception {
        // Create ingestion properties for JSON
        IngestRequestProperties jsonIngestProps = IngestRequestPropertiesBuilder
                .create(DATABASE, state.testTableName)
                .withEnableTracking(true)
                .build();

        // Use FileSource for streaming ingestion
        FileSource jsonSource = new FileSource(
                STREAMING_JSON_TEMP_FILE_PATH,
                Format.multijson,
                UUID.randomUUID(),
                CompressionType.NONE
        );

        // Ingest the JSON file
        ExtendedIngestResponse response = state.queuedIngestClient.ingestAsync(jsonSource, jsonIngestProps).get();
        String opId = response.getIngestResponse().getIngestionOperationId();

        // Create operation for tracking
        assert opId != null;
        IngestionOperation operation = new IngestionOperation(
                opId,
                DATABASE,
                state.testTableName,
                response.getIngestionType()
        );

        // Poll for completion (with 2 minute timeout)
        StatusResponse finalStatus = state.queuedIngestClient.pollForCompletion(
                operation,
                Duration.ofSeconds(5),
                Duration.ofMinutes(2)
        ).get();

        blackhole.consume(finalStatus.getStatus());
    }




    /**
     * Main method to run the benchmarks
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JavaBenchmarkTest.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
