package com.microsoft.azure.kusto.data;

import com.microsoft.aad.adal4j.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import javax.naming.ServiceUnavailableException;
import java.awt.*;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class AadAuthenticationHelper {

    private final static String DEFAULT_AAD_TENANT = "common";
    private final static String CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58";
    private final static String RESOURCE = "https://graph.windows.net";

    private ClientCredential clientCredential;
    private String userUsername;
    private String userPassword;
    private String clusterUrl;
    private String aadAuthorityUri;
    private AuthenticationType authenticationType;

    private enum AuthenticationType { USER, APPLICATION, DEVICE }

    AadAuthenticationHelper(@NotNull ConnectionStringBuilder csb) {
        clusterUrl = csb.getClusterUrl();
        if (StringUtils.isNotEmpty(csb.getApplicationClientId()) && StringUtils.isNotEmpty(csb.getApplicationKey())) {
            clientCredential = new ClientCredential(csb.getApplicationClientId(), csb.getApplicationKey());
            authenticationType = AuthenticationType.APPLICATION;
        } else if (StringUtils.isNotEmpty(csb.getUserUsername()) && StringUtils.isNotEmpty(csb.getUserPassword())) {
            userUsername = csb.getUserUsername();
            userPassword = csb.getUserPassword();
            authenticationType = AuthenticationType.USER;
        } else {
            authenticationType = AuthenticationType.DEVICE;
        }

        // Set the AAD Authority URI
        String aadAuthorityId = (csb.getAuthorityId() == null ? DEFAULT_AAD_TENANT : csb.getAuthorityId());
        aadAuthorityUri = String.format("https://login.microsoftonline.com/%s", aadAuthorityId);
    }

    String acquireAccessToken() throws DataServiceException  {
        try {
            switch (authenticationType) {
                case APPLICATION:
                    return acquireAadApplicationAccessToken().getAccessToken();
                case USER:
                    return acquireAadUserAccessToken().getAccessToken();
                case DEVICE:
                    return acquireAccessTokenUsingDeviceCodeFlow().getAccessToken();
                default:
                    throw new DataServiceException("Authentication type: " + authenticationType.name() + " is invalid");
            }
        } catch (Exception e) {
            throw new DataServiceException(e.getMessage());
        }

    }

    private AuthenticationResult acquireAadUserAccessToken() throws DataServiceException, DataClientException {
        AuthenticationContext context;
        AuthenticationResult result;
        ExecutorService service = null;
        try {
            service = Executors.newSingleThreadExecutor();
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
            service = Executors.newSingleThreadExecutor();
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

    AuthenticationResult acquireAccessTokenUsingDeviceCodeFlow() throws Exception {
        AuthenticationContext context = null;
        AuthenticationResult result = null;
        ExecutorService service = null;
        try {
            service = Executors.newSingleThreadExecutor();
            context = new AuthenticationContext( aadAuthorityUri, true, service);

            Future<DeviceCode> future = context.acquireDeviceCode(CLIENT_ID, RESOURCE, null);
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
            if (service != null) {
                service.shutdown();
            }
        }
        if (result == null) {
            throw new ServiceUnavailableException("authentication result was null");
        }
        return result;
    }

    public AuthenticationResult acquireWithClientCertificate(X509Certificate cert, PrivateKey privateKey)
            throws IOException, InterruptedException, ExecutionException, ServiceUnavailableException{

        AuthenticationContext context;
        AuthenticationResult result;
        ExecutorService service = null;

        try {
            service = Executors.newSingleThreadExecutor();
            context = new AuthenticationContext(aadAuthorityUri, false, service);
            AsymmetricKeyCredential asymmetricKeyCredential = AsymmetricKeyCredential.create(clientCredential.getClientId(),
                    privateKey, cert);
            // pass null value for optional callback function and acquire access token
            result = context.acquireToken(clusterUrl, asymmetricKeyCredential, null).get();
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }
        if (result == null) {
            throw new ServiceUnavailableException("authentication result was null");
        }
        return result;
    }

}
