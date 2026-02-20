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

---

### 3. ManagedStreamingIngestClient

**Purpose**: Intelligent hybrid that automatically chooses the best ingestion method.

**Decision Logic**:

```
┌─────────────────┐
│ Ingest Request  │
└────────┬────────┘
         │
         ▼
┌─────────────────────┐
│ Check Data Size     │
└────┬───────────┬────┘
     │           │
 > 10MB      <= 10MB
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
- **Refresh**: Automatically refreshes after 1 hour (configurable)
- **Contents**: Storage containers, lake folders, preferred upload method

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
Map<"database-table", ErrorState>
  where ErrorState = (timestamp, errorCategory)
```

**Error Categories**:
- `STREAMING_INGESTION_OFF`: Streaming disabled on cluster
- `TABLE_CONFIGURATION_PREVENTS_STREAMING`: Table doesn't support streaming
- `THROTTLED`: Server is throttling requests
- `TRANSIENT`: Temporary network/server issues
- `UNKNOWN`: Unexpected errors

**Backoff Periods**:
- **Throttled**: 10 seconds (configurable)
- **Streaming disabled**: 15 minutes (configurable)
- **Transient errors**: Immediate retry with exponential backoff

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

| Class | Responsibility |
|-------|----------------|
| `StreamingIngestClient` | Direct HTTP streaming ingestion |
| `QueuedIngestClient` | Queue-based ingestion with tracking |
| `ManagedStreamingIngestClient` | Intelligent routing between streaming and queued |

### Builder Layer

| Class | Responsibility |
|-------|----------------|
| `StreamingIngestClientBuilder` | Builds streaming client with validation |
| `QueuedIngestClientBuilder` | Builds queued client with uploader setup |
| `ManagedStreamingIngestClientBuilder` | Builds managed client with policy |

### Source Layer

| Class | Responsibility |
|-------|----------------|
| `IngestionSource` | Abstract base for all sources |
| `BlobSource` | Represents blob already in storage |
| `FileSource` | Represents local file |
| `StreamSource` | Represents in-memory stream |
| `LocalSource` | Abstract base for file/stream |

### Upload Layer

| Class | Responsibility |
|-------|----------------|
| `IUploader` | Interface for uploading to storage |
| `ManagedUploader` | Concrete uploader with retry logic |
| `ContainerUploaderBase` | Base class with container selection |
| `GzipCompressionStrategy` | GZIP compression implementation |
| `NoCompressionStrategy` | No-op compression for binary formats |

### Configuration Layer

| Class | Responsibility |
|-------|----------------|
| `ConfigurationCache` | Caches Kusto configuration |
| `ConfigurationClient` | Fetches configuration from DM endpoint |
| `ExtendedContainerInfo` | Wraps container URI with metadata |

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
| `DefaultManagedStreamingPolicy` | Default policy with state management |
| `ManagedStreamingErrorState` | Tracks error state per table |
| `IngestRetryPolicy` | Retry logic interface |
| `CustomRetryPolicy` | Configurable retry with backoff |
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
| `CONFIG_CACHE_DEFAULT_REFRESH_INTERVAL_HOURS` | 1 hour | Configuration refresh interval |
| `MANAGED_STREAMING_THROTTLE_BACKOFF_SECONDS` | 10 sec | Throttle backoff period |
| `MANAGED_STREAMING_RESUME_TIME_MINUTES` | 15 min | Resume streaming after disable |

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

