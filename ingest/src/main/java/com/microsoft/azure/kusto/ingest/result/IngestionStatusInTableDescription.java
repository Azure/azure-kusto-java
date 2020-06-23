// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.result;

import java.io.Serializable;

public class IngestionStatusInTableDescription implements Serializable {
    public String TableConnectionString;
    public String PartitionKey;
    public String RowKey;
}