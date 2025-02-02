// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import java.util.UUID;

public interface SourceInfo {
    /**
     * Checks that this SourceInfo is defined appropriately.
     */
    void validate();

    UUID getSourceId();

    void setSourceId(UUID sourceId);
}
