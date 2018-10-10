package com.microsoft.azure.kusto.data;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.aad.adal4j.DeviceCode;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import javax.naming.ServiceUnavailableException;
import java.awt.*;
import java.net.URI;
import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AadAuthenticationHelper {

    private final static String DEFAULT_AAD_TENANT = "common";
    private final static String CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58";
    private final static String RESOURCE = "https://graph.windows.net";

    private ClientCredential clientCredential;
    private String userUsername;
    private String userPassword;
    private String clusterUrl;
    private String aadAuthorityUri;

    public AadAuthenticationHelper(ConnectionStringBuilder csb) {
        clusterUrl = csb.getClusterUrl();

        if (!isNullOrEmpty(csb.getApplicationClientId()) && !isNullOrEmpty(csb.getApplicationKey())) {
            clientCredential = new ClientCredential(csb.getApplicationClientId(), csb.getApplicationKey());
        } else {
            userUsername = csb.getUserUsername();
            userPassword = csb.getUserPassword();
        }

        // Set the AAD Authority URI
        String aadAuthorityId = (csb.getAuthorityId() == null ? DEFAULT_AAD_TENANT : csb.getAuthorityId());
        aadAuthorityUri = String.format("https://login.microsoftonline.com/%s", aadAuthorityId);
    }

    public AuthenticationResult acquireAccessTokenUsingDeviceCodeFlow() throws Exception {
        AuthenticationContext context = null;
        AuthenticationResult result = null;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext( aadAuthorityUri, true, service);

            Future<DeviceCode> future = context.acquireDeviceCode(clientCredential.getClientId(), RESOURCE, null);
            DeviceCode deviceCode = future.get();
            System.out.println(deviceCode.getMessage());
            System.out.println("Press Enter after authenticating");
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().browse(new URI(deviceCode.getVerificationUrl()));
            }
            System.in.read();
            Future<AuthenticationResult> futureResult = context.acquireTokenByDeviceCode(deviceCode, null);
            result = futureResult.get();

        } finally {
            assert service != null;
            service.shutdown();
        }
        if (result == null) {
            throw new ServiceUnavailableException("authentication result was null");
        }
        return result;
    }

    String acquireAccessToken() throws DataServiceException, DataClientException {
        if (clientCredential != null) {
            return acquireAadApplicationAccessToken().getAccessToken();
        } else {
            return acquireAadUserAccessToken().getAccessToken();
        }
    }

    private AuthenticationResult acquireAadUserAccessToken() throws DataServiceException, DataClientException {
        AuthenticationContext context;
        AuthenticationResult result;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(aadAuthorityUri, true, service);

            Future<AuthenticationResult> future = context.acquireToken(
                    clusterUrl, CLIENT_ID, userUsername, userPassword,
                    null);
            result = future.get();
        } catch (InterruptedException | ExecutionException | MalformedURLException e) {
            throw new DataClientException(clusterUrl, "Error in acquiring UserAccessToken", e);
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }

        if (result == null) {
            throw new DataServiceException(clusterUrl, "acquireAadUserAccessToken got 'null' authentication result");
        }
        return result;
    }

    private AuthenticationResult acquireAadApplicationAccessToken() throws DataServiceException, DataClientException {
        AuthenticationContext context;
        AuthenticationResult result;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(aadAuthorityUri, true, service);
            Future<AuthenticationResult> future = context.acquireToken(clusterUrl, clientCredential, null);
            result = future.get();
        } catch (InterruptedException | ExecutionException | MalformedURLException e) {
            throw new DataClientException(clusterUrl, "Error in acquiring ApplicationAccessToken", e);
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }

        if (result == null) {
            throw new DataServiceException(clusterUrl, "acquireAadApplicationAccessToken got 'null' authentication result");
        }
        return result;
    }

    private Boolean isNullOrEmpty(String str) {
        return (str == null || str.trim().isEmpty());
    }
}