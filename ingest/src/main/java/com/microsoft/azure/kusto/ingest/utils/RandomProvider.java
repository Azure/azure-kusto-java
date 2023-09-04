package com.microsoft.azure.kusto.ingest.utils;

import java.util.List;

public interface RandomProvider {
    void shuffle(List<?> list);
}
