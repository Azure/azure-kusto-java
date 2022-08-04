package com.microsoft.azure.kusto.data.auth.endpoints;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class WellKnownKustoEndpointsData {

    @JsonIgnore
    public ArrayList<String> _Comments;
    public ArrayList<String> AllowedKustoSuffixes;
    public ArrayList<String> AllowedKustoHostnames;
    private static WellKnownKustoEndpointsData instance = null;
    public static WellKnownKustoEndpointsData getInstance() {
        if (instance == null)
            instance = readInstance();

        return instance;
    }
    public WellKnownKustoEndpointsData(){}

    private static WellKnownKustoEndpointsData readInstance(){
        try {
            String filename = System.getProperty("user.dir") + "/data/src/main/resources" +
                    "/WellKnownKustoEndpoints.json";
            ObjectMapper objectMapper = new ObjectMapper();
            byte[] bytes = Files.readAllBytes(Paths.get(filename));
//            FileInputStream file = new FileInputStream(fileName);
//            ObjectInputStream in = new ObjectInputStream(file);
            return objectMapper.readValue(bytes, WellKnownKustoEndpointsData.class);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}

