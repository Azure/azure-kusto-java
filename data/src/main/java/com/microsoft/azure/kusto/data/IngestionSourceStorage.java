package com.microsoft.azure.kusto.data;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class IngestionSourceStorage {
    public String sourceUri;

    public IngestionSourceStorage(String uri){
        sourceUri = uri;
    }

    public String toString(){
        ObjectMapper objectMapper = Utils.getObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
