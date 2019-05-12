package com.microsoft.azure.kusto.ingest;

class SecurityUtils {
    static String removeSecretsFromUrl(String url) {
        return url.split("[?]", 2)[0];
    }
}
