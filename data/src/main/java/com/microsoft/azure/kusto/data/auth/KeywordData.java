package com.microsoft.azure.kusto.data.auth;

public class KeywordData {
    public String name;
    public String type;
    public boolean secret;
    public String[] aliases;

    // For Deserialization
    public KeywordData() {
    }
}
