package com.microsoft.azure.kusto.ingest;

class SecretsHandler {
    static String removeSecretsFromUrl(String url){
        return url.split("[?]", 2)[0];
    }
}
