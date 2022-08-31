// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.storage.common.policy.RequestRetryOptions;

public interface QueuedIngestClient extends IngestClient {
    /**
     * Setter for QueueRequestOptions used by the client on adding ingest message to the Azure queue, read here
     * https://docs.microsoft.com/azure/data-explorer/kusto/api/netfx/about-kusto-ingest#ingest-client-flavors
     * about Kusto queued ingestion
     * @param queueRequestOptions - Options to use when creating QueueClient
     */
    void setQueueRequestOptions(RequestRetryOptions queueRequestOptions);
}
