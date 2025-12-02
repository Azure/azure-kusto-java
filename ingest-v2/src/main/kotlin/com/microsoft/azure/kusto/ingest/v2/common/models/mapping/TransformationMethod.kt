// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common.models.mapping

import kotlinx.serialization.Serializable as KSerializable

@KSerializable
enum class TransformationMethod {
    None,
    PropertyBagArrayToDictionary,
    SourceLocation,
    SourceLineNumber,
    GetPathElement,
    UnknownMethod,
    DateTimeFromUnixSeconds,
    DateTimeFromUnixMilliseconds,
}
