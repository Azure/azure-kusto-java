// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.aad.adal4j.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import javax.naming.ServiceUnavailableException;
import java.awt.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class AadAuthenticationHelper {

    private final static String DEFAULT_AAD_TENANT = "microsoft.com";
    private final static String CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58";
    final static long MIN_ACCESS_TOKEN_VALIDITY_IN_MILLISECS = 60000;

    private ClientCredential clientCredential;
    private String userUsername;
    private String userPassword;
    private String clusterUrl;
    private String aadAuthorityUri;
    private X509Certificate x509Certificate;
    private PrivateKey privateKey;
    private AuthenticationType authenticationType;
    private String accessToken;
    private AuthenticationResult lastAuthenticationResult;
    private Lock lastAuthenticationResultLock = new ReentrantLock();
    private String applicationClientId;

    private enum AuthenticationType {
        AAD_USERNAME_PASSWORD,
        AAD_APPLICATION_KEY,
        AAD_DEVICE_LOGIN,
        AAD_APPLICATION_CERTIFICATE,
        AAD_ACCESS_TOKEN
    }

    AadAuthenticationHelper(@NotNull ConnectionStringBuilder csb) throws URISyntaxException {

        URI clusterUri = new URI(csb.getClusterUrl());
        clusterUrl = String.format("%s://%s", clusterUri.getScheme(), clusterUri.getHost());
        if (StringUtils.isNotEmpty(csb.getApplicationClientId()) && StringUtils.isNotEmpty(csb.getApplicationKey())) {
            clientCredential = new ClientCredential(csb.getApplicationClientId(), csb.getApplicationKey());
            authenticationType = AuthenticationType.AAD_APPLICATION_KEY;
        } else if (StringUtils.isNotEmpty(csb.getUserUsername()) && StringUtils.isNotEmpty(csb.getUserPassword())) {
            userUsername = csb.getUserUsername();
            userPassword = csb.getUserPassword();
            authenticationType = AuthenticationType.AAD_USERNAME_PASSWORD;
        } else if (csb.getX509Certificate() != null && csb.getPrivateKey() != null) {
            x509Certificate = csb.getX509Certificate();
            privateKey = csb.getPrivateKey();
            applicationClientId = csb.getApplicationClientId();
            authenticationType = AuthenticationType.AAD_APPLICATION_CERTIFICATE;
        } else if (StringUtils.isNotBlank(csb.getAccessToken())) {
            authenticationType = AuthenticationType.AAD_ACCESS_TOKEN;
            accessToken = csb.getAccessToken();
        } else {
            authenticationType = AuthenticationType.AAD_DEVICE_LOGIN;
        }

        // Set the AAD Authority URI
        String aadAuthorityId = (csb.getAuthorityId() == null ? DEFAULT_AAD_TENANT : csb.getAuthorityId());
        String aadAuthorityFromEnv = System.getenv("AadAuthorityUri");
        if (aadAuthorityFromEnv == null){
            aadAuthorityUri = String.format("https://login.microsoftonline.com/%s", aadAuthorityId);
        } else {
            aadAuthorityUri = String.format("%s%s%s", aadAuthorityFromEnv, aadAuthorityFromEnv.endsWith("/") ? "" : "/", aadAuthorityId);
        }
    }

    String acquireAccessToken() throws DataServiceException {
        if (authenticationType == AuthenticationType.AAD_ACCESS_TOKEN) {
            return accessToken;
        }

        if (lastAuthenticationResult == null) {
            acquireToken();
        } else if (IsInvalidToken()) {
            if (lastAuthenticationResult.getRefreshToken() == null) {
                acquireToken();
            } else {
                lastAuthenticationResultLock.lock();
                try {
                    if (IsInvalidToken()) {
                        lastAuthenticationResult = acquireAccessTokenByRefreshToken();
                    }
                } finally {
                    lastAuthenticationResultLock.unlock();
                }
            }
        }

        return lastAuthenticationResult.getAccessToken();
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

    private AuthenticationResult acquireAccessTokenUsingDeviceCodeFlow() throws Exception {
        AuthenticationContext context = null;
        AuthenticationResult result = null;
        ExecutorService service = null;
        try {
            service = Executors.newSingleThreadExecutor();
            context = new AuthenticationContext(aadAuthorityUri, true, service);
            Future<DeviceCode> future = context.acquireDeviceCode(CLIENT_ID, clusterUrl, null);
            DeviceCode deviceCode = future.get();
            System.out.println(deviceCode.getMessage());
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().browse(new URI(deviceCode.getVerificationUrl()));
            }
            result = waitAndAcquireTokenByDeviceCode(deviceCode, context);


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

    private AuthenticationResult waitAndAcquireTokenByDeviceCode(DeviceCode deviceCode, AuthenticationContext context)
            throws InterruptedException {
        int timeout = 20 * 1000;
        AuthenticationResult result = null;
        while (timeout > 0) {
            try {
                Future<AuthenticationResult> futureResult = context.acquireTokenByDeviceCode(deviceCode, null);
                return futureResult.get();
            } catch (ExecutionException e) {
                Thread.sleep(1000);
                timeout -= 1000;
            }
        }
        return result;
    }

    AuthenticationResult acquireWithClientCertificate()
            throws InterruptedException, ExecutionException, ServiceUnavailableException {

        AuthenticationContext context;
        AuthenticationResult result = null;
        ExecutorService service = null;

        try {
            service = Executors.newSingleThreadExecutor();
            context = new AuthenticationContext(aadAuthorityUri, false, service);
            AsymmetricKeyCredential asymmetricKeyCredential = AsymmetricKeyCredential.create(applicationClientId,
                    privateKey, x509Certificate);
            // pass null value for optional callback function and acquire access token
            result = context.acquireToken(clusterUrl, asymmetricKeyCredential, null).get();
        } catch (MalformedURLException e) {
            e.printStackTrace();
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

    private void acquireToken() throws DataServiceException {
        lastAuthenticationResultLock.lock();
        try {
            if (IsInvalidToken()) {
                switch (authenticationType) {
                    case AAD_APPLICATION_KEY:
                        lastAuthenticationResult = acquireAadApplicationAccessToken();
                        break;
                    case AAD_USERNAME_PASSWORD:
                        lastAuthenticationResult = acquireAadUserAccessToken();
                        break;
                    case AAD_DEVICE_LOGIN:
                        lastAuthenticationResult = acquireAccessTokenUsingDeviceCodeFlow();
                        break;
                    case AAD_APPLICATION_CERTIFICATE:
                        lastAuthenticationResult = acquireWithClientCertificate();
                        break;
                    default:
                        throw new DataServiceException("Authentication type: " + authenticationType.name() + " is invalid");
                }
            }
        } catch (Exception e) {
            throw new DataServiceException(e.getMessage(), e);
        } finally {
            lastAuthenticationResultLock.unlock();
        }
    }

    private boolean IsInvalidToken() {
        return lastAuthenticationResult == null || lastAuthenticationResult.getExpiresOnDate().before(dateInAMinute());
    }

    AuthenticationResult acquireAccessTokenByRefreshToken() throws DataServiceException {
        AuthenticationContext context;
        ExecutorService service = null;

        try {
            service = Executors.newSingleThreadExecutor();
            context = new AuthenticationContext(aadAuthorityUri, false, service);
            switch (authenticationType) {
                case AAD_APPLICATION_KEY:
                case AAD_APPLICATION_CERTIFICATE:
                    return context.acquireTokenByRefreshToken(lastAuthenticationResult.getRefreshToken(), clientCredential, null).get();
                case AAD_USERNAME_PASSWORD:
                case AAD_DEVICE_LOGIN:
                    return context.acquireTokenByRefreshToken(lastAuthenticationResult.getRefreshToken(), CLIENT_ID, clusterUrl, null).get();
                default:
                    throw new DataServiceException("Authentication type: " + authenticationType.name() + " is invalid");
            }
        } catch (Exception e) {
            throw new DataServiceException(e.getMessage(), e);
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }
    }

    Date dateInAMinute() {
        return new Date(System.currentTimeMillis() + MIN_ACCESS_TOKEN_VALIDITY_IN_MILLISECS);
    }
}
