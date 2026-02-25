// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client.policy

import java.time.Instant

data class ManagedStreamingErrorState(
    val resetStateAt: Instant,
    val errorState: ManagedStreamingErrorCategory,
)
