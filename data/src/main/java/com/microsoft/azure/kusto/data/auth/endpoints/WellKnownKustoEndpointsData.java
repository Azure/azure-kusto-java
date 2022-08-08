package com.microsoft.azure.kusto.data.auth.endpoints;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.util.IOUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

public class WellKnownKustoEndpointsData {
    public static class AllowedEndpoints
    {
        public ArrayList<String> AllowedKustoSuffixes;
        public ArrayList<String> AllowedKustoHostnames;
    }

    @JsonIgnore
    public ArrayList<String> _Comments;
    public HashMap<String, AllowedEndpoints> AllowedEndpointsByLogin;
    private static WellKnownKustoEndpointsData instance = null;

    public static WellKnownKustoEndpointsData getInstance() {
        if (instance == null)
            instance = readInstance();

        return instance;
    }

    // For Deserialization
    public WellKnownKustoEndpointsData(){}

    private static WellKnownKustoEndpointsData readInstance(){
        try {
            // Beautiful !
            ObjectMapper objectMapper = new ObjectMapper();
            try (InputStream resourceAsStream = WellKnownKustoEndpointsData.class.getResourceAsStream(
                    "/WellKnownKustoEndpoints.json")) {
               return objectMapper.readValue(resourceAsStream, WellKnownKustoEndpointsData.class);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}

