# ingest-v2 — Porting Specification

> Language-agnostic specification for re-implementing the Kusto ingest-v2 module.
> Every constant, threshold, URL pattern, header, and behavioral rule is specified with exact values.
> Refer to `ingest-v2/ARCHITECTURE.md` for diagrams and Kotlin-specific class relationships.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Constants & Defaults](#2-constants--defaults)
3. [Public Interfaces](#3-public-interfaces)
4. [Data Sources](#4-data-sources)
5. [REST API Protocol](#5-rest-api-protocol)
6. [Queued Ingestion Behavior](#6-queued-ingestion-behavior)
7. [Streaming Ingestion Behavior](#7-streaming-ingestion-behavior)
8. [Managed Streaming Behavior](#8-managed-streaming-behavior)
9. [Upload Engine](#9-upload-engine)
10. [Configuration & Caching](#10-configuration--caching)
11. [Retry Policies](#11-retry-policies)
12. [Error Model](#12-error-model)
13. [Ingestion Mapping](#13-ingestion-mapping)
14. [Auth & Endpoint Security](#14-auth--endpoint-security)
15. [Request Properties](#15-request-properties)
16. [HTTP Client Configuration](#16-http-client-configuration)

---

## 1. Overview

The module provides three ingestion modes:

| Mode | Description |
|------|-------------|
| **Queued** | Upload data to Azure Blob/Lake → POST to the DM REST API to queue ingestion. Best for large or batch data. |
| **Streaming** | POST data directly to the Kusto engine. Low-latency. Limited to ~10 MiB (raw, before inflation factor). |
| **Managed Streaming** | Tries streaming first. Falls back to queued on failure, large data, or policy decision. Recommended default. |

### Component Map

An implementation MUST provide these logical components:

| Component | Responsibility |
|-----------|----------------|
| **IngestClient** | Core interface: `ingest`, `getOperationSummary`, `getOperationDetails` |
| **StreamingIngestClient** | Implements streaming ingestion |
| **QueuedIngestClient** | Implements queued ingestion (upload + POST) |
| **ManagedStreamingIngestClient** | Hybrid: streaming with queued fallback |
| **Uploader** | Uploads local data to Azure Blob Storage / Data Lake |
| **ConfigurationCache** | Caches DM configuration (containers, settings, refresh intervals) |
| **ManagedStreamingPolicy** | Decides when to skip streaming and go directly to queued |
| **Builder** (per client) | Fluent builder for each client type |

---

## 2. Constants & Defaults

### Upload

| Constant | Value | Usage |
|----------|-------|-------|
| `UPLOAD_BLOCK_SIZE_BYTES` | `4,194,304` (4 MiB) | Azure Blob upload block size |
| `UPLOAD_MAX_SINGLE_SIZE_BYTES` | `268,435,456` (256 MiB) | Azure Blob max single-upload threshold |
| `UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES` | `4,294,967,296` (4 GiB) | Default max data size per upload |
| `UPLOAD_CONTAINER_MAX_CONCURRENCY` | `4` | Default max parallel upload count |
| `BLOB_UPLOAD_TIMEOUT_HOURS` | `1` | Upload timeout |
| `MAX_BLOBS_PER_BATCH` | `70` | Fallback max blobs per queued ingest request |

### HTTP

| Constant | Value |
|----------|-------|
| `KUSTO_API_REQUEST_TIMEOUT_MS` | `60,000` |
| `KUSTO_API_CONNECT_TIMEOUT_MS` | `60,000` |
| `KUSTO_API_SOCKET_TIMEOUT_MS` | `60,000` |
| `KUSTO_API_VERSION` | `"2024-12-12"` |

### Streaming

| Constant | Value |
|----------|-------|
| `STREAMING_MAX_REQ_BODY_SIZE` | `10,485,760` (10 MiB) |

### Configuration Cache

| Constant | Value |
|----------|-------|
| `CONFIG_CACHE_DEFAULT_REFRESH_INTERVAL_HOURS` | `1` |
| `CONFIG_CACHE_DEFAULT_SKIP_SECURITY_CHECKS` | `false` |

### Retry

| Constant | Value |
|----------|-------|
| `INGEST_RETRY_POLICY_DEFAULT_INTERVAL_SECONDS` | `10` |
| `INGEST_RETRY_POLICY_DEFAULT_TOTAL_RETRIES` | `3` |
| `INGEST_RETRY_POLICY_CUSTOM_INTERVALS` | `[1s, 3s, 7s]` |

### Managed Streaming Defaults

| Constant | Value |
|----------|-------|
| `continueWhenStreamingIngestionUnavailable` | `false` |
| `dataSizeFactor` | `1.0` |
| `throttleBackoffSeconds` | `10` |
| `resumeMinutes` (time until resuming streaming) | `15` |
| `retryDelays` | `[1s, 2s, 4s]` |
| `retryJitterMs` | `1000` (0–999 ms random) |

### Status Polling Defaults

| Constant | Value |
|----------|-------|
| `pollingInterval` | `30s` |
| `timeout` | `5m` |

### HTTP Headers

All custom headers used by the SDK:

| Header | Description |
|--------|-------------|
| `x-ms-app` | Application name (tracing) |
| `x-ms-user` | User name (tracing) |
| `x-ms-client-version` | SDK version string |
| `x-ms-client-request-id` | `"KIC.execute;<uuid>"` per request |
| `x-ms-version` | API version (`"2024-12-12"`) |
| `x-ms-s2s-actor-authorization` | S2S token (Fabric Private Link) |
| `x-ms-fabric-s2s-access-context` | S2S access context (Fabric Private Link) |
| `Content-Type` | `application/json` (default) or `application/octet-stream` (streaming raw) |
| `Connection` | `keep-alive` |
| `Accept` | `application/json` |

---

## 3. Public Interfaces

### IngestClient

The core contract every client MUST implement:

```
ingest(source, database, table, properties?) → IngestionOperation
getOperationSummary(operation) → StatusResponse
getOperationDetails(operation) → StatusResponse
```

- All methods are async (coroutine/future/promise depending on language)
- Java-friendly wrappers that return `CompletableFuture` (or language equivalent) SHOULD be provided

### MultiIngestClient (extends IngestClient)

Additional batch API for clients that support multi-blob ingestion:

```
ingestMany(sources[], database, table, properties?) → IngestResults
getMaxBlobsPerBatch() → Int
```

- `QueuedIngestClient` and `ManagedStreamingIngestClient` implement this
- `StreamingIngestClient` does NOT (streaming is single-source only)

### IUploader

Upload contract for moving local data to Azure Storage:

```
upload(localSource) → BlobSource
uploadMany(localSources[]) → UploadResults
ignoreSizeLimit: Boolean
```

### IngestionOperation

Tracking token returned by `ingest()`:

```
operationId: String
database: String
table: String
ingestKind: STREAMING | QUEUED
```

---

## 4. Data Sources

### Type Hierarchy

```
IngestionSource (abstract)
├── LocalSource (abstract)
│   ├── FileSource      — backed by a file path
│   └── StreamSource    — backed by an input stream
└── BlobSource          — backed by an Azure Blob URL
```

### IngestionSource (base)

| Field | Type | Description |
|-------|------|-------------|
| `sourceId` | UUID | Auto-generated unique identifier |
| `name` | String | Human-readable name (derived from path/sourceId) |
| `format` | Format enum | Data format (csv, json, parquet, etc.) |
| `compressionType` | CompressionType | `GZIP`, `ZIP`, or `NONE` |

### LocalSource (extends IngestionSource)

| Method / Field | Description |
|----------------|-------------|
| `data()` | Returns an input stream of the source data |
| `size()` | Returns data size in bytes |
| `shouldCompress` | `true` unless format is binary (Avro, Parquet, ORC) or already compressed |
| `generateBlobName()` | Generates upload blob name |

### FileSource (extends LocalSource)

- `data()` opens the file; caches the stream; maps IO errors to `InvalidUploadStreamException`
- `size()` returns the file size from the filesystem
- Detects compression from file extension (`.gz` → GZIP, `.zip` → ZIP)

### StreamSource (extends LocalSource)

- Wraps an existing input stream
- `size()` uses `stream.available()` (may be approximate)

### BlobSource (extends IngestionSource)

- Represents an already-uploaded blob (returned after upload)
- `blobUrl` — full URL including SAS token
- `blobExactSize` — known size
- `getPathForTracing()` — strips SAS token from URL for safe logging

### CompressionType Enum

| Value | String suffix | Description |
|-------|---------------|-------------|
| `GZIP` | `"gz"` | Gzip compressed |
| `ZIP` | `"zip"` | Zip compressed |
| `NONE` | `""` | Uncompressed |

### Binary Formats (skip compression)

These formats MUST NOT be auto-compressed: `avro`, `apacheavro`, `parquet`, `orc`

---

## 5. REST API Protocol

Base URLs:
- **DM (Data Management)**: `https://ingest-{cluster}.kusto.windows.net`
- **Engine**: `https://{cluster}.kusto.windows.net`

All endpoints require Bearer token authentication.

---

### 5.1 Get Ingestion Configuration

```
GET /v1/rest/ingestion/configuration
Authorization: Bearer <token>
```

**Response** `200 OK`:

```json
{
  "containerSettings": {
    "containers": [
      { "path": "<blob-container-sas-url>" }
    ],
    "lakeFolders": [
      { "path": "<datalake-folder-sas-url>" }
    ],
    "refreshInterval": "<.NET TimeSpan string>",
    "preferredUploadMethod": "<string | null>"
  },
  "ingestionSettings": {
    "maxBlobsPerBatch": 70,
    "maxDataSize": 4294967296,
    "preferredIngestionMethod": "<string | null>"
  }
}
```

---

### 5.2 Submit Queued Ingestion

```
POST /v1/rest/ingestion/queued/{database}/{table}
Authorization: Bearer <token>
Content-Type: application/json
```

**Request body**:

```json
{
  "timestamp": "<ISO 8601>",
  "blobs": [
    {
      "url": "<blob-sas-url>",
      "sourceId": "<uuid>",
      "rawSize": 12345
    }
  ],
  "properties": {
    "format": "csv",
    "enableTracking": true,
    "tags": ["tag1", "ingest-by:tag2", "drop-by:tag3"],
    "ingestIfNotExists": ["tag1"],
    "skipBatching": false,
    "deleteAfterDownload": false,
    "ingestionMappingReference": "<name>",
    "ingestionMapping": "<json-string>",
    "validationPolicy": "<json-string>",
    "ignoreSizeLimit": false,
    "ignoreFirstRecord": false,
    "ignoreLastRecordIfInvalid": false,
    "creationTime": "<ISO 8601>",
    "zipPattern": "<regex>",
    "extend_schema": false,
    "recreate_schema": false
  }
}
```

**Response** `200 OK`:

```json
{
  "ingestionOperationId": "<uuid>"
}
```

**Error handling**:
- `404` → non-permanent `IngestException`
- Any other non-200 → permanent `IngestException`

---

### 5.3 Get Ingestion Status

```
GET /v1/rest/ingestion/queued/{database}/{table}/{operationId}?details={bool}
Authorization: Bearer <token>
```

**Response** `200 OK`:

```json
{
  "startTime": "<ISO 8601>",
  "lastUpdated": "<ISO 8601>",
  "status": {
    "succeeded": 1,
    "failed": 0,
    "inProgress": 0,
    "canceled": 0
  },
  "details": [
    {
      "sourceId": "<uuid>",
      "status": "Succeeded",
      "startedAt": "<ISO 8601>",
      "lastUpdateTime": "<ISO 8601>",
      "errorCode": null,
      "failureStatus": null,
      "details": null
    }
  ]
}
```

**BlobStatus.status** values: `Queued`, `InProgress`, `Succeeded`, `Failed`, `Canceled`

**BlobStatus.failureStatus** values: `Unknown`, `Permanent`, `Transient`, `Exhausted`

---

### 5.4 Streaming Ingestion

```
POST /v1/rest/ingest/{database}/{table}?streamFormat={format}&mappingName={name?}&sourceKind={uri?}
Authorization: Bearer <token>
Host: {engine-hostname}
Connection: Keep-Alive
```

**For raw data (FileSource / StreamSource)**:
```
Content-Type: application/octet-stream
Content-Encoding: gzip   (only if source compression is GZIP)
Body: <raw bytes>
```

**For blob reference (BlobSource)**:
```
Content-Type: application/json
sourceKind: uri
Body: {"SourceUri": "<blob-url>"}
```

**Response** `200 OK`: Success (ingestion accepted)

**Error response format** (OneAPI style):
```json
{
  "error": {
    "code": "<error-code>",
    "message": "<human-readable>",
    "@type": "<error-type>",
    "@message": "<detailed-message>",
    "@failureCode": "<numeric-code>",
    "@permanent": true
  }
}
```

- `@permanent` defaults to `true` when absent
- `404` → non-permanent, `failureSubCode = NETWORK_ERROR`
- Permanent errors → `IngestRequestException`
- Non-permanent errors → `IngestServiceException`

---

## 6. Queued Ingestion Behavior

### Flow

1. Validate format and check for duplicate blobs (by URL path before `?`)
2. If source is `LocalSource` → upload via `IUploader` → get `BlobSource`
3. Build `IngestRequest` with blob URLs, source IDs, sizes, and properties
4. POST to `/v1/rest/ingestion/queued/{database}/{table}`
5. Return `IngestionOperation` with the operation ID

### Multi-Blob Batching Rules

| Rule | Constraint |
|------|-----------|
| Sources list | MUST be non-empty |
| Max blobs per batch | `configurationResponse.ingestionSettings.maxBlobsPerBatch` or fallback `70` |
| Format consistency | All sources in a batch MUST share the same format |
| Duplicate detection | Compare blob paths (URL before `?` query string); reject duplicates |

### Status Polling

- Default polling interval: `30s`
- Default timeout: `5m`
- Completion: all blobs reach a terminal status (`Succeeded`, `Failed`, `Canceled`)
- If any blob fails after completion → permanent `IngestException`

---

## 7. Streaming Ingestion Behavior

### Size Gate

```
maxSize = STREAMING_MAX_REQ_BODY_SIZE × getRowStoreEstimatedFactor(format, compressionType)
```

If actual data size exceeds `maxSize`, throw permanent `IngestSizeLimitExceededException`.

The `getRowStoreEstimatedFactor` function returns an inflation/sizing factor based on the data format and compression type. This accounts for the fact that compressed data expands when stored in the row store.

### Source Handling

| Source Type | Body | Content-Type | sourceKind |
|-------------|------|-------------|------------|
| FileSource | Raw file bytes | `application/octet-stream` | _(none)_ |
| StreamSource | Raw stream bytes | `application/octet-stream` | _(none)_ |
| BlobSource | `{"SourceUri": "<url>"}` | `application/json` | `"uri"` |
| Other | Throw permanent `IngestException` | — | — |

### Error Parsing

Parse the response body as a OneAPI error object:
1. Extract `code`, `message`, `@type`, `@message`, `@failureCode`, `@permanent`
2. `@permanent` defaults to `true` if absent
3. Permanent → `IngestRequestException`; Non-permanent → `IngestServiceException`
4. HTTP `404` → override to non-permanent, `failureSubCode = NETWORK_ERROR`

---

## 8. Managed Streaming Behavior

### Decision Tree

```
1. Is data size > (STREAMING_MAX_REQ_BODY_SIZE × dataSizeFactor)?
   YES → go QUEUED immediately

2. Does policy.shouldDefaultToQueuedIngestion(db, table) return true?
   YES → go QUEUED immediately

3. Try STREAMING with retry policy:
   - Retry loop using CustomRetryPolicy [1s, 2s, 4s] + jitter [0–999ms]
   - throwOnExhaustedRetries = false

4. On streaming success:
   - Call policy.streamingSuccessCallback(details)
   - Return IngestionOperation (kind=STREAMING)

5. On streaming failure:
   - Classify the exception (see Error Classification below)
   - Call policy.streamingErrorCallback(details)
   - If retries remain and error is retryable → retry
   - Else → fall back to QUEUED
```

### Error Classification

Match exception message/code against these patterns (case-insensitive):

| Pattern | Category |
|---------|----------|
| Message contains `streaming` AND (`disabled` OR `not enabled` OR `off`) | `STREAMING_INGESTION_OFF` |
| Message contains `update policy` OR `schema` OR `incompatible` | `TABLE_CONFIGURATION_PREVENTS_STREAMING` |
| Message contains `too large` / `exceeds` / `maximum allowed size` / `KustoRequestPayloadTooLargeException` OR `failureCode == 413` | `REQUEST_PROPERTIES_PREVENT_STREAMING` |
| `failureCode == 429` OR message contains `KustoRequestThrottledException` | `THROTTLED` |
| Everything else | `OTHER_ERRORS` / `UNKNOWN_ERRORS` |

### Default Policy Fallback Windows

| Error Category | Fallback Duration | Notes |
|----------------|-------------------|-------|
| `STREAMING_INGESTION_OFF` | 15 minutes | Only falls back if `continueWhenStreamingIngestionUnavailable` is `true`; otherwise does NOT fall back (throws immediately) |
| `TABLE_CONFIGURATION_PREVENTS_STREAMING` | 15 minutes | Always falls back to queued |
| `REQUEST_PROPERTIES_PREVENT_STREAMING` | _(immediate)_ | Always falls back to queued |
| `THROTTLED` | 10 seconds | Falls back to queued during window |
| `OTHER_ERRORS` / `UNKNOWN_ERRORS` | _(no window)_ | Falls back to queued on exhausted retries |

### Error State

The policy maintains a per-table error state map:

```
Key: "{database}.{table}"
Value: { resetStateAt: Instant, errorState: ErrorCategory }
```

`shouldDefaultToQueuedIngestion()` returns `true` if the table has an active (non-expired) error state — except for `STREAMING_INGESTION_OFF` when continuation is disabled (returns `false` to force a retry / error).

### ManagedStreamingErrorCategory Enum

```
REQUEST_PROPERTIES_PREVENT_STREAMING
TABLE_CONFIGURATION_PREVENTS_STREAMING
STREAMING_INGESTION_OFF
THROTTLED
OTHER_ERRORS
UNKNOWN_ERRORS
```

---

## 9. Upload Engine

### Validation Rules

Before upload, validate the source in this order:

| Check | Condition | Error Code |
|-------|-----------|------------|
| Null stream | `data()` returns null | `SOURCE_IS_NULL` |
| Unreadable | `available()` throws or returns < 0 | `SOURCE_NOT_READABLE` |
| Empty | Size is `0` | `SOURCE_IS_EMPTY` |
| Too large | Size > `maxDataSize` AND `ignoreSizeLimit` is false | `SOURCE_SIZE_LIMIT_EXCEEDED` |

All validation failures throw `UploadFailedException` (permanent).

### Compression Decision

```
IF source.shouldCompress (i.e., not already compressed AND not a binary format):
    Apply GZIP compression
    Set effectiveCompression = GZIP
ELSE:
    No compression
    effectiveCompression = source.compressionType
```

**Binary formats** (skip compression): `avro`, `apacheavro`, `parquet`, `orc`

**Piped compression parameters**:
- Pipe buffer: `1 MiB`
- GZIP output buffer: `64 KiB`

### Blob Naming

```
"{sourceId}_{format}.{effectiveCompression}"
```

Example: `a1b2c3d4-e5f6-7890-abcd-ef1234567890_csv.gz`

If `effectiveCompression` is `NONE`, the suffix is empty: `a1b2c3d4-..._csv`

### Container Selection

`ManagedUploader` selects containers from the cached configuration:

| Condition | Container List |
|-----------|---------------|
| Only lake folders available | Use lake list |
| Only blob containers available | Use blob (storage) list |
| Both available, `uploadMethod == DEFAULT` | Check server's `preferredUploadMethod`; `"Lake"` (case-insensitive) → lake; else → storage |
| Both available, explicit `uploadMethod` | Use whichever is specified (`STORAGE` or `LAKE`) |
| No containers at all | Throw permanent `IngestException("No containers available")` |

### Round-Robin Container Selection

- Containers are wrapped in a `RoundRobinContainerList` with a shared atomic counter
- `getNextStartIndex()` returns `counter.getAndIncrement() % list.size`
- The counter is shared across all uploaders to distribute load evenly

### Upload Retry

- Uses the configured `IngestRetryPolicy`
- On each retry, advance to the next container via round-robin
- Permanent `IngestException` → stop retries immediately
- All retries exhausted → throw non-permanent `IngestException`

### Azure Blob Upload Parameters

| Parameter | Value |
|-----------|-------|
| Block size | 4 MiB |
| Max single upload size | 256 MiB |
| Timeout | 1 hour |

### Azure Data Lake Upload

- First URL path segment = filesystem / workspace ID
- Remaining segments = directory path
- Auth priority: token credential → SAS → anonymous
- Same size/concurrency/timeout settings as Blob

### Concurrent Batch Upload

`uploadMany()` runs uploads concurrently:
- Controlled by a semaphore with `maxConcurrency` permits (default: 4)
- Returns `UploadResults` with separate success/failure lists
- Each failure includes the source and error

---

## 10. Configuration & Caching

### Cache Structure

```
CachedData {
    configuration: ConfigurationResponse
    timestamp: Instant
    refreshInterval: Duration
    storageContainerList: RoundRobinContainerList   (from containers[])
    lakeContainerList: RoundRobinContainerList       (from lakeFolders[])
}
```

- Container lists are lazily built from the configuration response
- Each container is wrapped as `ExtendedContainerInfo(containerInfo, uploadMethod)` where upload method is `STORAGE` for containers and `LAKE` for lake folders

### Refresh Interval Calculation

```
effectiveRefreshInterval = min(CONFIG_CACHE_DEFAULT_REFRESH_INTERVAL_HOURS, parsedServerRefreshInterval)
```

- The server's `containerSettings.refreshInterval` is a .NET TimeSpan string
- If parsing fails, use the default (1 hour)

### .NET TimeSpan Parsing

Implementations MUST support these formats:
- `HH:mm:ss` (e.g., `01:00:00`)
- `d.HH:mm:ss` (e.g., `1.00:00:00`)
- `HH:mm:ss.fffffff` (e.g., `01:00:00.0000000`)

### Cache Behavior

- On first access, fetch configuration synchronously
- On subsequent accesses, return cached data if still fresh
- If refresh fails, return stale cached data (if any exists)
- Validation: if no custom provider is set, `dmUrl`, `tokenCredential`, and `skipSecurityChecks` MUST all be non-null

### S2S Token Provider

For Fabric Private Link scenarios, the cache supports an optional S2S token provider:
- Fetches an `S2SToken` that gets injected as headers in configuration requests
- Headers: `x-ms-s2s-actor-authorization` and `x-ms-fabric-s2s-access-context`

---

## 11. Retry Policies

### Policy Types

| Type | Description | Parameters |
|------|-------------|------------|
| `NoRetryPolicy` | Never retries | — |
| `SimpleRetryPolicy` | Fixed delay, N retries | `interval = 10s`, `totalRetries = 3` |
| `CustomRetryPolicy` | Variable delays from a list | `intervals = [1s, 3s, 7s]` |

### Retry Decision

```
moveNext(retryNumber) → RetryDecision { shouldRetry: Boolean, interval: Duration }
```

- `SimpleRetryPolicy`: retry while `retryNumber <= totalRetries`; interval is fixed
- `CustomRetryPolicy`: retry while `retryNumber < intervals.length`; interval from `intervals[retryNumber]` (zero-based)
- `NoRetryPolicy`: always returns `shouldRetry = false`, `interval = 0`

### `runWithRetry` Extension

The retry execution loop accepts these hooks:

| Hook | Description |
|------|-------------|
| `onRetry(retryNumber, delay)` | Called before each retry wait |
| `onError(exception)` | Called on each failure |
| `shouldRetry(exception)` | Return `false` to abort retries early |
| `tracer(message)` | Logging callback |
| `throwOnExhaustedRetries` | If `true`, throw after all retries exhausted; if `false`, return null |

The loop respects cancellation (coroutine cancellation / context cancellation) between retries.

### Managed Streaming Retry Policy

Used by `ManagedStreamingIngestClient`:
- Type: `CustomRetryPolicy`
- Intervals: `[1s, 2s, 4s]`
- Jitter: `0–999ms` added to each interval
- `throwOnExhaustedRetries = false` (falls back to queued instead of throwing)

---

## 12. Error Model

### Exception Hierarchy

```
IngestException (base)
├── IngestRequestException           — permanent request-side error
├── IngestServiceException           — temporary service-side error
├── IngestClientException            — client-side error
├── IngestSizeLimitExceededException — data too large
├── InvalidIngestionMappingException — bad mapping config
├── MultipleIngestionMappingPropertiesException — both reference + inline
├── UploadFailedException            — upload failure (uses UploadErrorCode)
│   ├── NoAvailableIngestContainersException  — no containers
│   ├── InvalidUploadStreamException          — invalid stream
│   └── UploadSizeLimitExceededException      — upload too large
```

### Exception Fields

| Field | Type | Description |
|-------|------|-------------|
| `message` | String | Human-readable error message |
| `cause` | Exception? | Underlying exception |
| `failureCode` | Int? | HTTP-style status code |
| `failureSubCode` | UploadErrorCode? | Detailed error classification |
| `isPermanent` | Boolean | Whether retries are pointless |

### Default Permanence

| Exception Type | Default `isPermanent` | Default `failureCode` |
|----------------|----------------------|----------------------|
| `IngestRequestException` | `true` | `null` |
| `IngestServiceException` | `false` | `500` |
| `IngestClientException` | `true` | `400` |
| `IngestSizeLimitExceededException` | `true` | — |
| `NoAvailableIngestContainersException` | `false` | `500` |
| `InvalidUploadStreamException` | `true` | — |
| `UploadSizeLimitExceededException` | `true` | — |

### Upload Error Codes

| Code Enum | String Value |
|-----------|-------------|
| `SOURCE_IS_NULL` | `UploadError_SourceIsNull` |
| `SOURCE_NOT_FOUND` | `UploadError_SourceNotFound` |
| `SOURCE_NOT_READABLE` | `UploadError_SourceNotReadable` |
| `SOURCE_IS_EMPTY` | `UploadError_SourceIsEmpty` |
| `SOURCE_SIZE_LIMIT_EXCEEDED` | `UploadError_SourceSizeLimitExceeded` |
| `UPLOAD_FAILED` | `UploadError_Failed` |
| `NO_CONTAINERS_AVAILABLE` | `UploadError_NoContainersAvailable` |
| `CONTAINER_UNAVAILABLE` | `UploadError_ContainerUnavailable` |
| `NETWORK_ERROR` | `UploadError_NetworkError` |
| `AUTHENTICATION_FAILED` | `UploadError_AuthenticationFailed` |
| `UNKNOWN` | `UploadError_Unknown` |

---

## 13. Ingestion Mapping

### Mapping Types

| Enum | Kusto Name | Associated Format |
|------|-----------|-------------------|
| `CSV` | `"Csv"` | `csv` |
| `JSON` | `"Json"` | `json` |
| `AVRO` | `"Avro"` | `avro` |
| `PARQUET` | `"Parquet"` | `parquet` |
| `SSTREAM` | `"SStream"` | `sstream` |
| `ORC` | `"Orc"` | `orc` |
| `APACHEAVRO` | `"ApacheAvro"` | `apacheavro` |
| `W3CLOGFILE` | `"W3CLogFile"` | `w3clogfile` |

### Mapping Modes

An ingestion request uses **one of** (never both):
1. **Mapping reference** — a named mapping policy stored on the Kusto table
2. **Inline mapping** — JSON-serialized column mapping array

If both are provided, throw permanent `IngestClientException` (failureCode `400`).

### Column Mapping Properties

| Property Key | Description |
|-------------|-------------|
| `Path` | JSON path / column path |
| `Transform` | Transformation method |
| `Ordinal` | Column ordinal (CSV) |
| `ConstValue` | Constant value |
| `Field` | Field name |
| `Columns` | Column list (Avro) |
| `StorageDataType` | Storage data type hint |

### Column Mapping Validation Rules

| Format(s) | Required |
|-----------|----------|
| `csv`, `sstream` | `columnName` MUST be non-blank |
| `json`, `parquet`, `orc`, `w3clogfile` | `columnName` MUST be non-blank AND (`path` is set OR transform is `SourceLineNumber` / `SourceLocation`) |
| `avro`, `apacheavro` | `columnName` MUST be non-blank AND `columns` MUST be non-blank |

### Transformation Methods

```
SourceLocation
SourceLineNumber
DateTimeFromUnixSeconds
DateTimeFromUnixMilliseconds
DateTimeFromUnixMicroseconds
DateTimeFromUnixNanoseconds
```

---

## 14. Auth & Endpoint Security

### URL Transformation

**Cluster URL → Ingestion URL** (`getIngestionEndpoint`):
- If URL is null, already contains `"ingest-"`, or is a reserved hostname → return unchanged
- Otherwise insert `"ingest-"` after `"://"` in the URL

**Ingestion URL → Query URL** (`getQueryEndpoint`):
- If URL is null or is a reserved hostname → return unchanged
- Otherwise strip the first occurrence of `"ingest-"` from the URL

### Reserved Hostnames

A hostname is "reserved" (skip URL transformation) if it is:
- `localhost`
- An IPv4 address (exactly 4 octets, each 0–255)
- An IPv6 bracketed address (e.g., `[::1]`)
- `onebox.dev.kusto.windows.net`
- Any URL that fails URI parsing or is not absolute

### Trusted Endpoint Validation

Before building any client, the target URL MUST be validated:

```
1. Is URL a loopback address? (localhost, 127.x.x.x, ::1, [::1])
   → TRUSTED

2. Does override matcher exist and match?
   → TRUSTED

3. Does URL match well-known Kusto endpoint rules?
   → TRUSTED (loaded from bundled WellKnownKustoEndpoints.json)

4. Does URL match additional user-registered trusted hosts?
   → TRUSTED

5. Is validation globally disabled?
   → LOG WARNING, allow

6. Otherwise:
   → THROW KustoClientInvalidConnectionStringException
```

### Override Mechanism

```
addTrustedHosts(rules, replace)
```
- `replace = true`: replaces the additional matcher entirely
- `replace = false`: merges new rules with existing additional matcher

```
setOverridePolicy(matcher)
```
- Replaces the entire validation with a custom matcher

### Fast Suffix Matcher

Trusted endpoint rules use a suffix matcher:
- Supports exact hostname matches
- Supports suffix matches (e.g., `*.kusto.windows.net`)
- Matchers can be merged for combining rule sets

### Default Login Endpoint

```
https://login.microsoftonline.com
```

---

## 15. Request Properties

### IngestRequestProperties Builder

All configurable properties for an ingest request:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `format` | Format | `csv` | Data format |
| `enableTracking` | Boolean? | — | Enable operation tracking |
| `additionalTags` | List\<String\> | `[]` | Extra tags |
| `dropByTags` | List\<String\> | `[]` | Drop-by tags (prefixed `drop-by:`) |
| `ingestByTags` | List\<String\> | `[]` | Ingest-by tags (prefixed `ingest-by:`) |
| `ingestIfNotExists` | List\<String\> | `[]` | Dedup tags |
| `skipBatching` | Boolean? | — | Skip server-side batching |
| `deleteAfterDownload` | Boolean? | — | Delete blob after ingestion |
| `ingestionMappingReference` | String? | — | Named mapping policy reference |
| `inlineIngestionMapping` | IngestionMapping? | — | Inline column mapping |
| `validationPolicy` | String? | — | Validation policy JSON |
| `ignoreSizeLimit` | Boolean? | — | Ignore size limits |
| `ignoreFirstRecord` | Boolean? | — | Skip first record (header) |
| `ignoreLastRecordIfInvalid` | Boolean? | — | Ignore invalid last record |
| `creationTime` | OffsetDateTime? | — | Extent creation time |
| `zipPattern` | String? | — | Regex for ZIP file selection |
| `extendSchema` | Boolean? | — | Auto-extend table schema |
| `recreateSchema` | Boolean? | — | Recreate table schema |

### Tag Handling

Combined tags array sent to the server is built in this order:

```
tags = additionalTags + ["ingest-by:{t}" for t in ingestByTags] + ["drop-by:{t}" for t in dropByTags]
```

### Build Validation

1. If both `ingestionMappingReference` and `inlineIngestionMapping` are set → throw permanent `IngestClientException` (failureCode `400`)
2. If `format` is not set → default to `csv`
3. If mapping provides a format, use it (if format wasn't explicitly set)

---

## 16. HTTP Client Configuration

### Base Client Setup

| Setting | Value |
|---------|-------|
| Request timeout | 60,000 ms |
| Connect timeout | 60,000 ms |
| Socket timeout | 60,000 ms |
| JSON: ignore unknown keys | `true` |
| JSON: lenient parsing | `true` |
| Auth scope | `"{dmUrl}/.default"` |
| Client request ID format | `"KIC.execute;<uuid>"` |

### Default Headers (every request)

```
Content-Type: application/json
x-ms-app: <appName>
x-ms-user: <userName>
x-ms-client-version: <sdkVersion>
x-ms-client-request-id: KIC.execute;<uuid>
x-ms-version: 2024-12-12
Connection: keep-alive
Accept: application/json
```

### Client Details (Tracing)

The `ClientDetails` object constructs tracing values:
- App name, user, version are sent as `x-ms-app`, `x-ms-user`, `x-ms-client-version`
- Defaults are auto-detected from the runtime environment
- Can be overridden via builder: `withClientDetails(appName, user)`

### Fabric Private Link Headers

When S2S auth is configured, inject:
```
x-ms-s2s-actor-authorization: Bearer <s2s-token>
x-ms-fabric-s2s-access-context: <access-context>
```

---

## Appendix A: Supported Data Formats

```
csv, tsv, scsv, sohsv, psv, txt, raw, tsve,
json, singlejson, multijson,
avro, apacheavro,
parquet, orc, sstream,
w3clogfile, azmonstream
```

## Appendix B: Batch Operation Result

Any batch operation (upload, ingest) returns a result implementing:

```
successes: List<SuccessType>
failures: List<FailureType>
hasFailures: Boolean      → failures.isNotEmpty()
allSucceeded: Boolean     → failures.isEmpty()
totalCount: Int           → successes.size + failures.size
```

## Appendix C: IngestKind Enum

```
STREAMING   — data was ingested via streaming
QUEUED      — data was ingested via queued path
```

Reported in `IngestionOperation` and `ExtendedIngestResponse` so callers know which path was used.
