package com.microsoft.azure.kusto.data;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import java.time.Instant;
import java.util.Date;

import javax.naming.ServiceUnavailableException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AadAuthenticationHelper {

    private final static String DEFAULT_AAD_TENANT = "common";
    private final static String KUSTO_CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58";

    private ClientCredential clientCredential;
    private String userUsername;
    private String userPassword;
    private String clusterUrl;
    private String aadAuthorityId;
    private String aadAuthorityUri;
    
    private AuthenticationResult cachedToken;

    AadAuthenticationHelper(KustoConnectionStringBuilder kcsb) {
        clusterUrl = kcsb.getClusterUrl();

        if (!isNullOrEmpty(kcsb.getApplicationClientId()) && !isNullOrEmpty(kcsb.getApplicationKey())) {
            clientCredential = new ClientCredential(kcsb.getApplicationClientId(), kcsb.getApplicationKey());
        } else {
            userUsername = kcsb.getUserUsername();
            userPassword = kcsb.getUserPassword();
        }

        // Set the AAD Authority URI
        aadAuthorityId = (kcsb.getAuthorityId() == null ? DEFAULT_AAD_TENANT : kcsb.getAuthorityId());
        aadAuthorityUri = String.format("https://login.microsoftonline.com/%s", aadAuthorityId);
    }

    String acquireAccessToken(Date renewalTime) throws Exception {
        if (cachedToken != null && renewalTime.before(cachedToken.getExpiresOnDate())){
            return cachedToken.getAccessToken();
        }
        if (clientCredential != null) {
            cachedToken = acquireAadApplicationAccessToken();
        } else {
            cachedToken = acquireAadUserAccessToken();
        }
        return cachedToken.getAccessToken();
    }
    
    String acquireAccessToken() throws Exception {
       return acquireAccessToken(Date.from(Instant.now().minusSeconds(60)));
    }

    private AuthenticationResult acquireAadUserAccessToken() throws Exception {
        AuthenticationContext context;
        AuthenticationResult result;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(aadAuthorityUri, true, service);

            Future<AuthenticationResult> future = context.acquireToken(
                    clusterUrl, KUSTO_CLIENT_ID, userUsername, userPassword,
                    null);
            result = future.get();
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }

        if (result == null) {
            throw new ServiceUnavailableException("acquireAadUserAccessToken got 'null' authentication result");
        }
        return result;
    }

    private AuthenticationResult acquireAadApplicationAccessToken() throws Exception {
        AuthenticationContext context;
        AuthenticationResult result;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(aadAuthorityUri, true, service);
            Future<AuthenticationResult> future = context.acquireToken(clusterUrl, clientCredential, null);
            result = future.get();
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }

        if (result == null) {
            throw new ServiceUnavailableException("acquireAadApplicationAccessToken got 'null' authentication result");
        }
        return result;
    }

    private Boolean isNullOrEmpty(String str) {
        return (str == null || str.trim().isEmpty());
    }
}
