package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import java.util.Collections;
import java.util.List;

public class IngestionStatusResult implements IngestionResult {

    private IngestionStatus ingestionStatus;

    public IngestionStatusResult(IngestionStatus ingestionStatus) {
        this.ingestionStatus = ingestionStatus;
    }

    @Override
    public List<IngestionStatus> getIngestionStatusCollection() {
        return Collections.singletonList(this.ingestionStatus);
    }

    @Override
    public int getIngestionStatusesLength() {
        return 1;
    }
}