package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.utils.TimeProvider;

public class MockTimeProvider implements TimeProvider  {
    private long currentTimeMillis;

    public MockTimeProvider(long currentTimeMillis) {
        this.currentTimeMillis = currentTimeMillis;
    }

    @Override
    public long currentTimeMillis() {
        return currentTimeMillis;
    }

    public void setCurrentTimeMillis(long currentTimeMillis) {
        this.currentTimeMillis = currentTimeMillis;
    }

}
