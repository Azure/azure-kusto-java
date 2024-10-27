package com.microsoft.azure.kusto.data;

import com.fasterxml.jackson.core.JsonProcessingException;

class IngestionSourceStorage {
    public String sourceUri;

    public IngestionSourceStorage(String uri) {
        sourceUri = uri;
    }

    public String toString() {
        try {
            return Utils.getObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
