// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploaders

/** Specifies the upload method to use for blob uploads. */
enum class UploadMethod {
    /** Use server preference or Storage as fallback. */
    DEFAULT,

    /** Use Azure Storage blob. */
    STORAGE,

    /** Use OneLake. */
    LAKE,
}
