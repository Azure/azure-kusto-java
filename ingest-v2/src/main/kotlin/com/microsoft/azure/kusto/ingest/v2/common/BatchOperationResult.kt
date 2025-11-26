// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

/**
 * Common interface for batch operation results that track successes and
 * failures.
 */
interface BatchOperationResult<S, F> {
    val successes: List<S>
    val failures: List<F>

    /** Returns true if any operations failed. */
    val hasFailures: Boolean
        get() = failures.isNotEmpty()

    /** Returns true if all operations succeeded (no failures). */
    val allSucceeded: Boolean
        get() = failures.isEmpty()

    /** Returns the total count of operations (successes + failures). */
    val totalCount: Int
        get() = successes.size + failures.size
}
