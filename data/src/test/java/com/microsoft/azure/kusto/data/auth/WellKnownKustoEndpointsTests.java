package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.auth.endpoints.WellKnownKustoEndpointsData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.HashSet;

import static com.microsoft.azure.kusto.data.auth.CloudInfo.*;

public class WellKnownKustoEndpointsTests {
    @Test
    @DisplayName("Validate loading of WellKnownKustoEndpointsData")public void GetWellKnownKustoDnsSuffixes()
    {
        // Has to be final
        class Holder {
            boolean v;
        }
        Holder locateOneExpectedResource = new Holder();

        WellKnownKustoEndpointsData.getInstance().AllowedEndpointsByLogin.forEach((key, value) -> {
            // Validate suffixes start with a dot sign (.)
            value.AllowedKustoSuffixes.forEach(suffix -> Assertions.assertTrue(suffix.startsWith("."),
                    String.format("A DNS Suffix '%s' must start with dot (.) sign", suffix)));

            // Check for duplicates
            HashSet<String> suffixSet = new HashSet<>(value.AllowedKustoSuffixes);
            int diff = value.AllowedKustoSuffixes.size() - suffixSet.size();
            Assertions.assertEquals(0, diff, String.format("There are: %d duplicate DNS suffixes. Login endpoint is: " +
                    "%s.", diff, key));

            // Search for one expected URL in one of the lists
            locateOneExpectedResource.v |= suffixSet.contains(".kusto.windows.net");
        });

        // Locate one expected resource
        if (!locateOneExpectedResource.v)
        {
            Assertions.fail("kusto.windows.net is not found in the list of allowed suffixes");
        }
    }

    @Test
    @DisplayName("validate auth with certificate throws exception when missing or invalid parameters")
    void failForInvalidLogin() throws URISyntaxException {
        CloudInfo.manuallyAddToCache("https://resource.uri", new CloudInfo(
                DEFAULT_LOGIN_MFA_REQUIRED,
                "https://InvalidLogin.uri",
                DEFAULT_KUSTO_CLIENT_APP_ID,
                DEFAULT_REDIRECT_URI,
                DEFAULT_KUSTO_SERVICE_RESOURCE_ID,
                DEFAULT_FIRST_PARTY_AUTHORITY_URL));

    }

    @Test
    @DisplayName("validate auth with certificate throws exception when missing or invalid parameters")
    void throwFirstButWorkAfterAddingOverride() throws URISyntaxException {

    }
}
