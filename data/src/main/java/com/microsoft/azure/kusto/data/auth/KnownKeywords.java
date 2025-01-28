package com.microsoft.azure.kusto.data.auth;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum KnownKeywords {
    DATA_SOURCE("Data Source", true),
    INITIAL_CATALOG("Initial Catalog", true),
    FEDERATED_SECURITY("AAD Federated Security", true),
    APPLICATION_CLIENT_ID("Application Client Id", true),
    APPLICATION_KEY("Application Key", true),
    USER_ID("User ID", true),
    AUTHORITY_ID("Authority Id", true),
    APPLICATION_TOKEN("Application Token", true),
    USER_TOKEN("User Token", true),
    APPLICATION_CERTIFICATE_X5C("Application Certificate SendX5c", true),
    APPLICATION_NAME_FOR_TRACING("Application Name for Tracing", true),
    USER_NAME_FOR_TRACING("User Name for Tracing", true),
    PASSWORD("Password", false),
    APPLICATION_CERTIFICATE_BLOB("Application Certificate Blob", false), // TODO - if we add a way to pass a certificate by a byte[], support this
    APPLICATION_CERTIFICATE_THUMBPRINT("Application Certificate Thumbprint", false),
    DSTS_FEDERATED_SECURITY("dSTS Federated Security", false),
    STREAMING("Streaming", false),
    UNCOMPRESSED("Uncompressed", false),
    ENFORCE_MFA("EnforceMfa", false),
    ACCEPT("Accept", false),
    QUERY_CONSISTENCY("Query Consistency", false),
    DATA_SOURCE_URI("Data Source Uri", false),
    AZURE_REGION("Azure Region", false),
    NAMESPACE("Namespace", false),
    APPLICATION_CERTIFICATE_ISSUER_DISTINGUISHED_NAME("Application Certificate Issuer Distinguished Name", false),
    APPLICATION_CERTIFICATE_SUBJECT_DISTINGUISHED_NAME("Application Certificate Subject Distinguished Name", false);

    private final String canonicalName;
    private final boolean isSupported;
    private String type;
    private boolean isSecret;

    public static final Map<String, KnownKeywords> knownKeywords = Arrays.stream(KnownKeywords.values()).collect(HashMap::new,
            (map, keyword) -> map.put(keyword.canonicalName, keyword), HashMap::putAll);

    KnownKeywords(String canonicalName, boolean isSupported) {
        this.canonicalName = canonicalName;
        this.isSupported = isSupported;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isSecret() {
        return isSecret;
    }

    public void setSecret(boolean secret) {
        isSecret = secret;
    }

    public boolean isSupported() {
        return isSupported;
    }

    public String getCanonicalName() {
        return canonicalName;
    }

}
