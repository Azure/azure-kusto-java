package com.microsoft.azure.kusto.quickstart;

/**
 * SourceType - represents the type of files used for ingestion
 */
enum SourceType {
    localFileSource("Local File Source"),
    blobSource("Blob Source");

    private String source;


    SourceType(String source) {
        this.source = source;
    }

    public String getSource() {
        return source;
    }
}

/**
 * AuthenticationModeOptions - represents the different options to autenticate to the system
 */
enum AuthenticationModeOptions {
    userPrompt("UserPrompt"),
    managedIdentity("ManagedIdentity"),
    appKey("AppKey"),
    appCertificate("AppCertificate");

    private String mode;

    AuthenticationModeOptions(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}

public class SampleApp {
}
