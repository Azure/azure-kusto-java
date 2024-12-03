// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import reactor.core.publisher.Mono;

import java.io.Closeable;

public interface IngestClient extends Closeable {

    /**
     * <p>Ingest data from a file into Kusto database.</p>
     * This method ingests the data from a given file, described in {@code fileSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param fileSourceInfo      The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @see FileSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties);

    Mono<IngestionResult> ingestFromFileAsync(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties);

    /**
     * <p>Ingest data from a blob storage into Kusto database.</p>
     * This method ingests the data from a given blob, described in {@code blobSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param blobSourceInfo      The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @see BlobSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties);

    Mono<IngestionResult> ingestFromBlobAsync(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties);

    /**
     * <p>Ingest data from a Result Set into Kusto database.</p>
     * This method ingests the data from a given Result Set, described in {@code resultSetSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     * <p>
     * Ingesting from ResultSet is equivalent to ingesting from a csv stream.
     * The DataFormat should be empty or set to "csv", and the mapping, should it be provided, should be csv mapping.
     *
     * @param resultSetSourceInfo The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @see ResultSetSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties);

    Mono<IngestionResult> ingestFromResultSetAsync(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties);

    /**
     * <p>Ingest data from an input stream, into Kusto database.</p>
     * This method ingests the data from a given input stream, described in {@code streamSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param streamSourceInfo    The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @see StreamSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties);

    Mono<IngestionResult> ingestFromStreamAsync(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties);
}
