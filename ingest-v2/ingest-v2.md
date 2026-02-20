# Kusto Ingest-V2 Architecture Documentation

## Overview

The **ingest-v2** module is a modern, Kotlin-based ingestion SDK for Azure Data Explorer (Kusto). It provides three types of ingestion clients:

1. **StreamingIngestClient** - Direct, synchronous ingestion for low-latency scenarios
2. **QueuedIngestClient** - Reliable, asynchronous ingestion with operation tracking
3. **ManagedStreamingIngestClient** - Intelligent hybrid that automatically chooses between streaming and queued ingestion

---

## Table of Contents

1. [Core Components](#core-components)
2. [Ingestion Source Types](#ingestion-source-types)
3. [Client Types](#client-types)
4. [Queued Ingestion Flow](#queued-ingestion-flow)
5. [Streaming Ingestion Flow](#streaming-ingestion-flow)
6. [Managed Streaming Ingestion Flow](#managed-streaming-ingestion-flow)
7. [Key Classes and Responsibilities](#key-classes-and-responsibilities)

---

## Core Components

### Class Hierarchy

```
┌─────────────────────────────────────────────┐
│         IngestionSource (abstract)          │
│  - format, compressionType, sourceId        │
└───────────────┬─────────────────────────────┘
                │
        ┌───────┴────────┐
        │                │
┌───────▼───────┐  ┌─────▼──────┐
│  LocalSource  │  │ BlobSource │
│  (abstract)   │  │            │
└───────┬───────┘  └────────────┘
        │
    ┌───┴────┐
    │        │
┌───▼───┐ ┌─▼──────┐
│FileSource│ │StreamSource│
└─────────┘ └────────┘
```

### IngestClient Hierarchy

```
┌──────────────────────────────┐
│    IngestClient (interface)  │
│  - ingestAsync()             │
│  - getOperationSummaryAsync()│
│  - getOperationDetailsAsync()│
└──────────────┬───────────────┘
               │
     ┌─────────┼──────────────────┐
     │         │                  │
┌────▼─────┐ ┌─▼─────────┐ ┌──────▼────────────────┐
│Streaming │ │  Queued   │ │ ManagedStreaming      │
│Ingest    │ │  Ingest   │ │ IngestClient          │
│Client    │ │  Client   │ │ (uses both)           │
└──────────┘ └───────────┘ └───────────────────────┘
```

---

## Ingestion Source Types

### 1. **BlobSource**
- Represents data already in Azure Blob Storage
- Contains blob URL (with optional SAS token)
- No upload required - data is already accessible

### 2. **FileSource** (extends LocalSource)
- Represents a local file on disk
- Provides file path and file size
- Automatically determines format from file extension

### 3. **StreamSource** (extends LocalSource)
- Represents in-memory data stream
- Supports any `InputStream`
- Size may be unknown until stream is read

### Key Properties
- **format**: Data format (CSV, JSON, Parquet, etc.)
- **compressionType**: Compression applied (NONE, GZIP, ZIP)
- **sourceId**: Unique identifier for tracking
- **shouldCompress**: Whether to compress during upload (binary formats are not compressed)

---

## Client Types

### 1. StreamingIngestClient

**Purpose**: Direct, synchronous ingestion with immediate response.

**Best for**:
- Small datasets (< 4MB recommended)
- Low-latency requirements
- Real-time scenarios

**Limitations**:
- No operation tracking
- Size limits (10MB max)
- May be throttled by server

**Usage**:
```kotlin
val client = StreamingIngestClientBuilder.create(engineUrl)
    .withAuthentication(tokenProvider)
    .build()

val response = client.ingestAsync(source, properties)
```

---

### 2. QueuedIngestClient

**Purpose**: Reliable, asynchronous ingestion through Azure Storage queues.

**Best for**:
- Large datasets (GB scale)
- Batch ingestion
- When operation tracking is required
- Guaranteed delivery

**How it works**:
1. Uploads data to Azure Blob Storage
2. Posts messages to Azure Queue Storage
3. Kusto service processes queue messages
4. Provides tracking through status tables

**Usage**:
```kotlin
val client = QueuedIngestClientBuilder.create(dmUrl)
    .withAuthentication(tokenProvider)
    .build()

val response = client.ingestAsync(source, properties)
// Track operation
val status = client.getOperationSummaryAsync(response.operation)
```

**Operation Tracking**:

Queued ingestion returns an `IngestionOperation` that can be used to track the ingestion status:

```kotlin
data class IngestionOperation(
    val operationId: String,      // Unique identifier
    val database: String,          // Target database
    val table: String,             // Target table
    val ingestKind: IngestKind     // STREAMING or QUEUED
)
```

Two tracking methods are available:
- `getOperationSummaryAsync(operation)`: Returns aggregated status counts
- `getOperationDetailsAsync(operation)`: Returns detailed status with individual file/blob details

**Note**: Streaming ingestion operations cannot be tracked. Calling tracking methods on streaming operations will return empty results with a warning.

---

### 3. ManagedStreamingIngestClient

**Purpose**: Intelligent hybrid that automatically chooses the best ingestion method.

**Decision Logic**:

The managed streaming client uses a size threshold calculated as:
```
sizeThreshold = STREAMING_MAX_REQ_BODY_SIZE * dataSizeFactor
                = 10 MB * 1.0 (default)
                = 10 MB
```

This threshold can be adjusted via the `dataSizeFactor` parameter in the policy.

```
┌─────────────────┐
│ Ingest Request  │
└────────┬────────┘
         │
         ▼
┌─────────────────────┐
│ Check Data Size     │
│ (> threshold?)      │
└────┬───────────┬────┘
     │           │
   > 10MB     <= 10MB
     │           │
     │           ▼
     │    ┌──────────────────┐
     │    │ Check Policy     │
     │    │ State            │
     │    └────┬──────────┬──┘
     │    Should    Try
     │    use       streaming
     │    queued        │
     │         │        │
     │         │        ▼
     │         │  ┌────────────────┐
     │         │  │ Attempt        │
     │         │  │ Streaming      │
     │         │  └────┬───────────┘
     │         │       │
     │         │       ▼
     │         │  ┌────────────────┐
     │         │  │ Streaming      │
     │         │  │ Result?        │
     │         │  └─┬──────┬───────┘
     │         │   Success │
     │         │    │      │
     │         │    │   ┌──▼───────────┐
     │         │    │   │ Error Type?  │
     │         │    │   └┬────────┬───┬┘
     │         │    │    │        │   │
     │         │    │ Throttled/  │ Transient
     │         │    │ Disabled/   │
     │         │    │ Too Large   │
     │         │    │    │        │   │
     │         │    │    │        │   ▼
     │         │    │    │        │ ┌────────┐
     │         │    │    │        │ │ Retry? │
     │         │    │    │        │ └┬──────┬┘
     │         │    │    │        │ Yes   No
     │         │    │    │        │  │     │
     │         │    │    │        │  │     │
     │         │    │    │        │ Loop   │
     │         │    │    │        │ back   │
     ▼         ▼    │    ▼        ▼   to   ▼
┌──────────────────────────────────────────┐
│      Use Queued Ingestion                │
└──────────────┬───────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────┐
│      Return Success Response             │
└──────────────────────────────────────────┘
```

**Key Features**:
- Automatic fallback from streaming to queued
- Policy-based decision making
- Retry logic with exponential backoff
- State tracking per database-table combination

---

## Queued Ingestion Flow

### End-to-End Flow

```
┌──────────────┐
│  User Code   │
└──────┬───────┘
       │ ingestAsync(LocalSource, props)
       ▼
┌─────────────────────┐
│ QueuedIngestClient  │
└──────┬──────────────┘
       │ 1. Upload phase
       ▼
┌─────────────────────┐
│   IUploader         │
│ (ManagedUploader)   │
└──────┬──────────────┘
       │ 2. Get containers
       ▼
┌─────────────────────┐
│ ConfigurationCache  │◄──── Fetches from Kusto DM endpoint
└──────┬──────────────┘
       │ 3. Returns container URIs
       ▼
┌─────────────────────┐
│  ManagedUploader    │
│  - Compress data    │
│  - Upload to blob   │
└──────┬──────────────┘
       │ 4. Returns BlobSource
       ▼
┌─────────────────────┐
│ QueuedIngestClient  │
│ - Build IngestRequest
│ - Post to queue     │
└──────┬──────────────┘
       │ 5. API call
       ▼
┌─────────────────────┐
│ KustoBaseApiClient  │───► POST /v1/rest/ingestion/ingest
└──────┬──────────────┘
       │ 6. Returns operation ID
       ▼
┌─────────────────────┐
│ ExtendedIngest      │
│ Response            │
└─────────────────────┘
```

### Key Classes

#### ConfigurationCache
- **Purpose**: Caches Kusto configuration (containers, queues, settings)
- **Refresh**: Automatically refreshes based on minimum of default interval (1 hour) and server-provided refresh interval
- **Contents**: Storage containers, lake folders, preferred upload method
- **Refresh Interval Parsing**: Supports .NET TimeSpan format (e.g., "01:00:00" for 1 hour, "1.02:30:00" for 1 day 2.5 hours)
- **Thread Safety**: Uses atomic updates to prevent duplicate fetches during concurrent refresh attempts
- **Failure Handling**: Returns cached data if refresh fails (when available)

#### ManagedUploader (implements IUploader)
- **Responsibilities**:
  - Select storage containers (blob or lake)
  - Compress data (if needed)
  - Upload files/streams to Azure Storage
  - Handle retry logic
  - Support concurrent uploads

- **Upload Process**:
  1. Determine if compression is needed
  2. Select target container (blob or lake)
  3. Upload with block-level parallelism (4MB blocks)
  4. Return BlobSource with uploaded location

#### Compression Strategy
```
┌──────────────────────────────┐
│   CompressionStrategy        │
│   (interface)                │
└───────┬──────────────────────┘
        │
    ┌───┴──────────┐
    │              │
┌───▼───────┐ ┌────▼──────────┐
│  Gzip     │ │NoCompression  │
│Compression│ │Strategy       │
└───────────┘ └───────────────┘
```

- **GzipCompressionStrategy**: Compresses stream using GZIP
- **NoCompressionStrategy**: Pass-through (for binary formats or pre-compressed data)

---

## Streaming Ingestion Flow

### End-to-End Flow

```
┌──────────────┐
│  User Code   │
└──────┬───────┘
       │ ingestAsync(source, props)
       ▼
┌─────────────────────────┐
│ StreamingIngestClient   │
└──────┬──────────────────┘
       │ 1. Prepare request
       ▼
┌─────────────────────────┐
│ Convert source to       │
│ InputStream             │
└──────┬──────────────────┘
       │ 2. HTTP POST
       ▼
┌─────────────────────────┐
│ KustoBaseApiClient      │───► POST /v1/rest/ingest/{db}/{table}
└──────┬──────────────────┘      Content-Type: application/json
       │ 3. Immediate response    Body: compressed data stream
       ▼
┌─────────────────────────┐
│ Parse response          │
│ - Success/Failure       │
│ - Error details         │
└──────┬──────────────────┘
       │ 4. Return result
       ▼
┌─────────────────────────┐
│ ExtendedIngestResponse  │
└─────────────────────────┘
```

### Key Points

- **Synchronous**: Ingestion happens immediately
- **Size Limit**: Maximum 10MB request body (configurable)
- **No Tracking**: Cannot track operation status later
- **Format Support**: All formats supported (CSV, JSON, Parquet, Avro, etc.)
- **Compression**: Data compressed in-flight for efficiency

### Error Handling

Streaming can fail with various error categories:
- **Transient**: Network errors, timeouts → Retry
- **Permanent**: Streaming disabled, data too large → Should fallback
- **Throttled**: Server overloaded → Should fallback with backoff

---

## Managed Streaming Ingestion Flow

### Decision Flow Diagram

```
┌─────────────────────────────────────────────────────────┐
│         ManagedStreamingIngestClient                    │
└───────────────────┬─────────────────────────────────────┘
                    │
                    ▼
         ┌──────────────────────┐
         │ 1. Check Data Size   │
         └──────────┬───────────┘
                    │
         ┌──────────▼──────────┐
         │ Size > 10MB?        │
         └──┬──────────────┬───┘
           YES             NO
            │              │
            │              ▼
            │   ┌────────────────────┐
            │   │ 2. Check Policy    │
            │   │ shouldDefaultTo    │
            │   │ QueuedIngestion()  │
            │   └────┬───────────┬───┘
            │       YES          NO
            │        │           │
            │        │           ▼
            │        │  ┌──────────────────┐
            │        │  │ 3. Try Streaming │
            │        │  └────┬─────────────┘
            │        │       │
            │        │       ▼
            │        │  ┌──────────────────┐
            │        │  │  Result?         │
            │        │  └─┬──────────┬─────┘
            │        │   SUCCESS   FAILURE
            │        │    │          │
            │        │    │          ▼
            │        │    │    ┌──────────────┐
            │        │    │    │ Permanent    │
            │        │    │    │ Error?       │
            │        │    │    └─┬─────────┬──┘
            │        │    │     YES       NO
            │        │    │      │         │
            │        │    │      │         ▼
            │        │    │      │   ┌───────────┐
            │        │    │      │   │ Retry     │
            │        │    │      │   │ (with     │
            │        │    │      │   │ backoff)  │
            │        │    │      │   └─────┬─────┘
            │        │    │      │         │
            │        │    │      │   ┌─────▼──────┐
            │        │    │      │   │Retries     │
            │        │    │      │   │Exhausted?  │
            │        │    │      │   └┬──────────┬┘
            │        │    │      │   YES        NO
            │        │    │      │    │         │
            │        │    │      │    │    Loop back
            │        │    │      │    │    to Try
            │        │    │      │    │   Streaming
            ▼        ▼    │      ▼    ▼
         ┌──────────────────────────────┐
         │  4. Use Queued Ingestion     │
         └──────────────┬───────────────┘
                        │
                        ▼
         ┌──────────────────────────────┐
         │  Return ExtendedIngestResponse│
         └──────────────────────────────┘
```

### Policy: DefaultManagedStreamingPolicy

The policy maintains state per database-table combination:

**State Management**:
```kotlin
// Concurrent map tracking error states
Map<"database-table", ManagedStreamingErrorState>
  where ManagedStreamingErrorState = 
    data class(resetStateAt: Instant, errorState: ManagedStreamingErrorCategory)
```

**Error Categories**:
- `STREAMING_INGESTION_OFF`: Streaming disabled on cluster (service-level configuration issue)
- `TABLE_CONFIGURATION_PREVENTS_STREAMING`: Table doesn't support streaming (table-specific configuration)
- `REQUEST_PROPERTIES_PREVENT_STREAMING`: Request properties make streaming unsuitable (request-specific)
- `THROTTLED`: Server is throttling requests (HTTP 429)
- `OTHER_ERRORS`: All other types of streaming errors
- `UNKNOWN_ERRORS`: Unexpected error types occurred

**Backoff Periods**:
- **Throttled**: 10 seconds (configurable via `MANAGED_STREAMING_THROTTLE_BACKOFF_SECONDS`)
- **Streaming disabled**: 15 minutes (configurable via `MANAGED_STREAMING_RESUME_TIME_MINUTES`)
- **Transient errors**: Uses retry delays of 1s, 2s, 4s with random jitter (0-1000ms) from `MANAGED_STREAMING_RETRY_DELAYS_SECONDS`

### Callbacks

The policy provides callbacks for monitoring:

**streamingSuccessCallback**:
- Called on successful streaming ingestion
- Can be used for metrics collection

**streamingErrorCallback**:
- Called on streaming failure
- Updates policy state for future decisions
- Triggers backoff timers

---

## Key Classes and Responsibilities

### Client Layer

| Class                          | Responsibility                                   |
|--------------------------------|--------------------------------------------------|
| `StreamingIngestClient`        | Direct HTTP streaming ingestion                  |
| `QueuedIngestClient`           | Queue-based ingestion with tracking              |
| `ManagedStreamingIngestClient` | Intelligent routing between streaming and queued |

### Builder Layer

| Class                                 | Responsibility                           |
|---------------------------------------|------------------------------------------|
| `StreamingIngestClientBuilder`        | Builds streaming client with validation  |
| `QueuedIngestClientBuilder`           | Builds queued client with uploader setup |
| `ManagedStreamingIngestClientBuilder` | Builds managed client with policy        |

### Source Layer

| Class             | Responsibility                     |
|-------------------|------------------------------------|
| `IngestionSource` | Abstract base for all sources      |
| `BlobSource`      | Represents blob already in storage |
| `FileSource`      | Represents local file              |
| `StreamSource`    | Represents in-memory stream        |
| `LocalSource`     | Abstract base for file/stream      |

### Upload Layer

| Class | Responsibility |
|-------|----------------|
| `IUploader` | Interface for uploading to storage (Kotlin coroutines + Java CompletableFuture support) |
| `ICustomUploader` | Pure Java interface for custom uploaders using CompletableFuture |
| `ManagedUploader` | Concrete uploader with retry logic and container selection |
| `ContainerUploaderBase` | Base class with container selection and upload orchestration |
| `GzipCompressionStrategy` | GZIP compression implementation |
| `NoCompressionStrategy` | No-op compression for binary formats |
| `RoundRobinContainerList` | Thread-safe round-robin container selection with shared atomic counter across all uploaders using the same cache |
| `UploadMethod` | Enum for upload method (STORAGE, LAKE, DEFAULT) |

**Note on RoundRobinContainerList**: The counter is stored within the `RoundRobinContainerList` instance, so all uploaders sharing the same `ConfigurationCache` (and thus the same `RoundRobinContainerList` instances via `CachedConfigurationData`) will distribute their uploads evenly across containers. This ensures true round-robin distribution across the entire application, not just within a single uploader instance.

### Configuration Layer

| Class | Responsibility |
|-------|----------------|
| `ConfigurationCache` | Interface for caching Kusto configuration |
| `DefaultConfigurationCache` | Default time-based configuration cache with atomic refresh logic |
| `CachedConfigurationData` | Wrapper around ConfigurationResponse with shared RoundRobinContainerList instances |
| `ConfigurationClient` | Fetches configuration from DM endpoint |
| `ExtendedContainerInfo` | Wraps container URI with metadata and upload method |
| `ConfigurationResponse` | Response model from configuration endpoint |

### API Layer

| Class | Responsibility |
|-------|----------------|
| `KustoBaseApiClient` | Base HTTP client for Kusto APIs |
| `IngestRequestProperties` | Properties for ingestion request |
| `IngestRequest` | Request body for queued ingestion |
| `IngestResponse` | Response from ingestion API |
| `StatusResponse` | Operation status response |

### Policy Layer

| Class | Responsibility |
|-------|----------------|
| `ManagedStreamingPolicy` | Interface for streaming decisions |
| `DefaultManagedStreamingPolicy` | Default policy with state management and backoff logic |
| `ManagedStreamingErrorState` | Tracks error state per table with resetStateAt and errorState |
| `ManagedStreamingErrorCategory` | Enum for error categories (STREAMING_INGESTION_OFF, TABLE_CONFIGURATION_PREVENTS_STREAMING, REQUEST_PROPERTIES_PREVENT_STREAMING, THROTTLED, OTHER_ERRORS, UNKNOWN_ERRORS) |
| `ManagedStreamingRequestSuccessDetails` | Details about successful streaming request (duration) |
| `ManagedStreamingRequestFailureDetails` | Details about failed streaming request (duration, isPermanent, errorCategory, exception) |
| `IngestRetryPolicy` | Retry logic interface |
| `CustomRetryPolicy` | Configurable retry with backoff and jitter |
| `SimpleRetryPolicy` | Simple fixed-interval retry |

---

## Configuration and Constants

Key constants defined in `IngestV2.kt`:

| Constant | Value | Description |
|----------|-------|-------------|
| `STREAMING_MAX_REQ_BODY_SIZE` | 10 MB | Maximum streaming request size |
| `UPLOAD_BLOCK_SIZE_BYTES` | 4 MB | Block size for blob uploads |
| `UPLOAD_MAX_SINGLE_SIZE_BYTES` | 256 MB | Max size for single-block upload |
| `MAX_BLOBS_PER_BATCH` | 70 | Maximum blobs in one queue message |
| `CONFIG_CACHE_DEFAULT_REFRESH_INTERVAL_HOURS` | 1 hour | Configuration cache refresh interval |
| `MANAGED_STREAMING_THROTTLE_BACKOFF_SECONDS` | 10 sec | Throttle backoff period |
| `MANAGED_STREAMING_RESUME_TIME_MINUTES` | 15 min | Resume streaming after disable |
| `MANAGED_STREAMING_RETRY_DELAYS_SECONDS` | [1, 2, 4] | Retry delays array (in seconds) |
| `MANAGED_STREAMING_RETRY_JITTER_MS` | 1000 ms | Maximum jitter to add to retry delays |
| `MANAGED_STREAMING_DATA_SIZE_FACTOR_DEFAULT` | 1.0 | Data size factor for queued threshold |
| `MANAGED_STREAMING_CONTINUE_WHEN_UNAVAILABLE_DEFAULT` | false | Default behavior when streaming unavailable |
| `UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES` | 4 GB | Default max data size for blob uploads |
| `UPLOAD_CONTAINER_MAX_CONCURRENCY` | 4 | Default max concurrency for uploads |
| `STREAM_COMPRESSION_BUFFER_SIZE_BYTES` | 64 KB | Buffer size for compression streams |
| `STREAM_PIPE_BUFFER_SIZE_BYTES` | 1 MB | Buffer size for piped streams |
| `KUSTO_API_VERSION` | "2024-12-12" | Kusto API version used in requests |
| `KUSTO_API_REQUEST_TIMEOUT_MS` | 60,000 ms | HTTP request timeout |
| `KUSTO_API_CONNECT_TIMEOUT_MS` | 60,000 ms | HTTP connection timeout |
| `KUSTO_API_SOCKET_TIMEOUT_MS` | 60,000 ms | HTTP socket timeout |
| `INGEST_RETRY_POLICY_DEFAULT_INTERVAL_SECONDS` | 10 sec | SimpleRetryPolicy interval |
| `INGEST_RETRY_POLICY_DEFAULT_TOTAL_RETRIES` | 3 | SimpleRetryPolicy total retries |
| `INGEST_RETRY_POLICY_CUSTOM_INTERVALS` | [1, 3, 7] | CustomRetryPolicy intervals (seconds) |
| `BLOB_UPLOAD_TIMEOUT_HOURS` | 1 hour | Blob upload timeout |

---

## Error Handling

The ingest-v2 module provides comprehensive error handling with clear distinction between permanent and transient errors:

### Exception Types

```
IngestException (base exception)
    │
    ├─ IngestClientException (client-side errors)
    │   - isPermanent: Boolean flag
    │   - Used for validation, configuration errors
    │
    └─ Other exceptions (network, I/O, etc.)
```

### Permanent vs Transient Errors

**Permanent Errors** (no retry):
- Streaming disabled on cluster
- Table configuration prevents streaming
- Invalid request properties (format mismatch, etc.)
- Authorization failures

**Transient Errors** (retry with backoff):
- Network timeouts
- Throttling (HTTP 429)
- Temporary server errors (HTTP 5xx)
- Connection failures

### Retry Policies

Three retry policy implementations are available:

1. **SimpleRetryPolicy**
   - Fixed interval between retries
   - Default: 10 seconds interval, 3 total retries
   
2. **CustomRetryPolicy**
   - Configurable delays: [1s, 3s, 7s] by default
   - No jitter

3. **ManagedStreamingPolicy.retryPolicy**
   - Delays: [1s, 2s, 4s]
   - Adds random jitter (0-1000ms) to prevent thundering herd

### Error Response Parsing

The SDK parses Kusto OneApiError responses to extract:
- Error code
- Error message
- Permanent flag
- Additional context

Example error response handling:
```kotlin
try {
    val response = client.ingestAsync(database, table, source, props)
} catch (e: IngestClientException) {
    if (e.isPermanent) {
        // Don't retry - fix the issue (e.g., enable streaming, fix format)
        logger.error("Permanent ingestion error: ${e.message}")
    } else {
        // Transient error - retry may succeed
        logger.warn("Transient ingestion error: ${e.message}")
    }
}
```

---

## Java Interoperability

The ingest-v2 module is written in Kotlin but provides first-class Java support:

### Async Methods with CompletableFuture

All client interfaces provide both Kotlin suspend functions and Java-friendly methods returning `CompletableFuture`:

**Kotlin:**
```kotlin
suspend fun ingestAsync(
    database: String, 
    table: String, 
    source: IngestionSource, 
    props: IngestRequestProperties?
): ExtendedIngestResponse
```

**Java:**
```java
CompletableFuture<ExtendedIngestResponse> ingestAsyncJava(
    String database,
    String table, 
    IngestionSource source, 
    IngestRequestProperties props
)
```

### Custom Uploaders

Java developers can implement custom uploaders in two ways:

1. **Recommended: ICustomUploader** (Pure Java)
   - Uses `CompletableFuture` instead of Kotlin coroutines
   - Simpler for Java developers
   - Example:
   ```java
   public class MyCustomUploader implements ICustomUploader {
       @Override
       public CompletableFuture<BlobSource> uploadAsync(LocalSource local) {
           // Your upload logic here
           return CompletableFuture.completedFuture(blobSource);
       }
       
       @Override
       public CompletableFuture<UploadResults> uploadManyAsync(List<LocalSource> sources) {
           // Batch upload logic
           return CompletableFuture.completedFuture(results);
       }
   }
   
   // Wrap it to use with the client
   IUploader uploader = CustomUploaderHelper.asUploader(new MyCustomUploader());
   ```

2. **Direct: IUploader** (Requires Kotlin understanding)
   - Implement `suspend fun` methods directly
   - More powerful but requires Kotlin coroutine knowledge

### Builder Pattern

All clients use builders with fluent API that works seamlessly from Java:

```java
ManagedStreamingIngestClient client = ManagedStreamingIngestClientBuilder
    .create("https://mycluster.kusto.windows.net")
    .withAuthentication(tokenProvider)
    .withPolicy(customPolicy)
    .build();
```

### Factory Methods

Static factory methods are provided via `@JvmStatic` for Java convenience:

```java
ManagedUploader uploader = ManagedUploader.builder()
    .withConfigurationCache(cache)
    .withMaxConcurrency(8)
    .build();

DefaultConfigurationCache cache = DefaultConfigurationCache.create(
    dmUrl,
    tokenCredential,
    clientDetails
);
```

---

## Summary

The ingest-v2 module provides a flexible, modern ingestion solution:

✅ **Three client types** for different scenarios  
✅ **Automatic fallback** with ManagedStreamingIngestClient  
✅ **Intelligent retry logic** with exponential backoff  
✅ **Efficient uploads** with parallel block-level transfers  
✅ **State management** for optimal streaming decisions  
✅ **Comprehensive error handling** with permanent vs transient classification  
✅ **Configuration caching** to minimize API calls  
✅ **Multiple source types** (file, stream, blob)  
✅ **Built with Kotlin coroutines** for async operations  
✅ **Java interop** with CompletableFuture support  

For typical usage, **ManagedStreamingIngestClient** is recommended as it provides the best of both worlds with automatic optimization.

