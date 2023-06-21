// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import java.io.File;

public class TestDataItem {
    public File file;
    public IngestionProperties ingestionProperties;
    public int rows;
    public boolean testOnstreamingIngestion = true;
    public boolean testOnManaged = true;
}
