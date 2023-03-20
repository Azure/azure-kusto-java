package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.auth.endpoints.KustoTrustedEndpoints;
import com.microsoft.azure.kusto.data.auth.endpoints.MatchRule;
import com.microsoft.azure.kusto.data.auth.endpoints.WellKnownKustoEndpointsData;
import com.microsoft.azure.kusto.data.exceptions.KustoClientInvalidConnectionStringException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;

import static com.microsoft.azure.kusto.data.auth.CloudInfo.*;

public class WellKnownKustoEndpointsTests {
    private final static String chinaCloudLogin = "https://login.partner.microsoftonline.cn";

    @Test
    @DisplayName("Validate loading of WellKnownKustoEndpointsData")
    public void getWellKnownKustoDnsSuffixes() {
        // Has to be final
        class BooleanHolder {
            boolean v;
        }
        BooleanHolder locateSuffix = new BooleanHolder();
        BooleanHolder locateHostname = new BooleanHolder();

        WellKnownKustoEndpointsData.getInstance().AllowedEndpointsByLogin.forEach((key, value) -> {
            // Validate suffixes start with a dot sign (.)
            value.AllowedKustoSuffixes.forEach(suffix -> Assertions.assertTrue(suffix.startsWith("."),
                    String.format("A DNS Suffix '%s' must start with dot (.) sign", suffix)));

            // Check for duplicates
            HashSet<String> suffixSet = new HashSet<>(value.AllowedKustoSuffixes);
            HashSet<String> hostSet = new HashSet<>(value.AllowedKustoHostnames);
            int diff = value.AllowedKustoSuffixes.size() - suffixSet.size();
            Assertions.assertEquals(0, diff, String.format("There are: %d duplicate DNS suffixes. Login endpoint is: " +
                    "%s.", diff, key));

            // Search for one expected URL in one of the lists
            locateSuffix.v |= suffixSet.contains(".kusto.windows.net");
            locateHostname.v |= hostSet.contains("kusto.aria.microsoft.com");
        });

        // Locate one expected resource
        if (!locateSuffix.v) {
            Assertions.fail("kusto.windows.net is not found in the list of allowed suffixes");
        }

        if (!locateHostname.v) {
            Assertions.fail("kusto.aria.microsoft.com is not found in the list of allowed hostnames");
        }
    }

    @Test
    @DisplayName("Validate some endpoints")
    public void wellKnownKustoEndpoints_RandomKustoClustersTest() throws KustoClientInvalidConnectionStringException, URISyntaxException {
        for (String c : new String[] {
                "https://127.0.0.1",
                "https://127.1.2.3",
                "https://kustozszokb5yrauyq.westeurope.kusto.windows.net",
                "https://kustofrbwrznltavls.centralus.kusto.windows.net",
                "https://kusto7j53clqswr4he.germanywestcentral.kusto.windows.net",
                "https://rpe2e0422132101fct2.eastus2euap.kusto.windows.net",
                "https://kustooq2gdfraeaxtq.westcentralus.kusto.windows.net",
                "https://kustoesp3ewo4s5cow.westcentralus.kusto.windows.net",
                "https://kustowmd43nx4ihnjs.southeastasia.kusto.windows.net",
                "https://createt210723t0601.westus2.kusto.windows.net",
                "https://kusto2rkgmaskub3fy.eastus2.kusto.windows.net",
                "https://kustou7u32pue4eij4.australiaeast.kusto.windows.net",
                "https://kustohme3e2jnolxys.northeurope.kusto.windows.net",
                "https://kustoas7cx3achaups.southcentralus.kusto.windows.net",
                "https://rpe2e0104160100act.westus2.kusto.windows.net",
                "https://kustox5obddk44367y.southcentralus.kusto.windows.net",
                "https://kustortnjlydpe5l6u.canadacentral.kusto.windows.net",
                "https://kustoz74sj7ikkvftk.southeastasia.kusto.windows.net",
                "https://rpe2e1004182350fctf.westus2.kusto.windows.net",
                "https://rpe2e1115095448act.westus2.kusto.windows.net",
                "https://kustoxenx32x3tuznw.southafricawest.kusto.windows.net",
                "https://kustowc3m5jpqtembw.canadacentral.kusto.windows.net",
                "https://rpe2e1011182056fctf.westus2.kusto.windows.net",
                "https://kusto3ge6xthiafqug.eastus.kusto.windows.net",
                "https://teamsauditservice.westus.kusto.windows.net",
                "https://kustooubnzekmh4doy.canadacentral.kusto.windows.net",
                "https://rpe2e1206081632fct2f.westus2.kusto.windows.net",
                "https://stopt402211020t0606.automationtestworkspace402.kusto.azuresynapse.net",
                "https://delt402210818t2309.automationtestworkspace402.kusto.azuresynapse.net",
                "https://kusto42iuqj4bejjxq.koreacentral.kusto.windows.net",
                "https://kusto3rv75hibmg6vu.southeastasia.kusto.windows.net",
                "https://kustogmhxb56nqjrje.westus2.kusto.windows.net",
                "https://kustozu5wg2p3aw3um.koreasouth.kusto.windows.net",
                "https://kustos36f2amn2agwk.australiaeast.kusto.windows.net",
                "https://kustop4htq3k676jau.eastus.kusto.windows.net",
                "https://kustojdny5lga53cts.southcentralus.kusto.windows.net",
                "https://customerportalprodeast.kusto.windows.net",
                "https://rpe2e0730231650und.westus2.kusto.windows.net",
                "https://kusto7lxdbebadivjw.southeastasia.kusto.windows.net",
                "https://alprd2neu000003s.northeurope.kusto.windows.net",
                "https://kustontnwqy3eler5g.northeurope.kusto.windows.net",
                "https://kustoap2wpozj7qpio.eastus.kusto.windows.net",
                "https://kustoajnxslghxlee4.japaneast.kusto.windows.net",
                "https://oiprdseau234x.australiasoutheast.kusto.windows.net",
                "https://kusto7yevbo7ypsnx4.germanywestcentral.kusto.windows.net",
                "https://kustoagph5odbqyquq.westus3.kusto.windows.net",
                "https://kustovs2hxo3ftud5e.westeurope.kusto.windows.net",
                "https://kustorzuk2dgiwdryc.uksouth.kusto.windows.net",
                "https://kustovsb4ogsdniwqk.eastus2.kusto.windows.net",
                "https://kusto3g3mpmkm3p3xc.switzerlandnorth.kusto.windows.net",
                "https://kusto2e2o7er7ypx2o.westus2.kusto.windows.net",
                "https://kustoa3qqlh23yksim.southafricawest.kusto.windows.net",
                "https://rpe2evnt11021711comp.rpe2evnt11021711-wksp.kusto.azuresynapse.net",
                "https://cdpkustoausas01.australiasoutheast.kusto.windows.net",
                "https://testinge16cluster.uksouth.kusto.windows.net",
                "https://testkustopoolbs6ond.workspacebs6ond.kusto.azuresynapse.net",
                "https://offnodereportingbcdr1.southcentralus.kusto.windows.net",
                "https://mhstorage16red.westus.kusto.windows.net",
                "https://kusto7kza5q2fmnh2w.northeurope.kusto.windows.net",
                "https://tvmquerycanc.centralus.kusto.windows.net",
                "https://kustowrcde4olp4zho.eastus.kusto.windows.net",
                "https://delt403210910t0727.automationtestworkspace403.kusto.azuresynapse.net",
                "https://foprdcq0004.brazilsouth.kusto.windows.net",
                "https://rpe2e0827133746fctf.eastus2euap.kusto.windows.net",
                "https://kustoz7yrvoaoa2yaa.australiaeast.kusto.windows.net",
                "https://rpe2e1203125809und.westus2.kusto.windows.net",
                "https://kustoywilbpggrltk4.francecentral.kusto.windows.net",
                "https://stopt402210825t0408.automationtestworkspace402.kusto.azuresynapse.net",
                "https://kustonryfjo5klvrh4.westeurope.kusto.windows.net",
                "https://kustowwqgogzpseg6o.eastus2.kusto.windows.net",
                "https://kustor3gjpwqum3olw.canadacentral.kusto.windows.net",
                "https://dflskfdslfkdslkdsfldfs.westeurope.kusto.data.microsoft.com",
                "https://dflskfdslfkdslkdsfldfs.westeurope.kusto.fabric.microsoft.com",
        }) {
            {
                validateEndpoint(c, DEFAULT_PUBLIC_LOGIN_URL);
            }

            // Test case sensitivity
            {
                String clusterName = c.toUpperCase();
                validateEndpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL);
            }

            String[] specialUrls = new String[] {
                    "synapse",
                    "kusto.data.microsoft.com",
                    "kusto.fabric.microsoft.com",
            };

            // Test MFA endpoints
            if (Arrays.stream(specialUrls).noneMatch(c::contains)) {
                String clusterName = c.replace(".kusto.", ".kustomfa.");
                validateEndpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL);
            }

            // Test dev endpoints
            if (Arrays.stream(specialUrls).noneMatch(c::contains)) {
                String clusterName = c.replace(".kusto.", ".kustodev.");
                validateEndpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL);
            }
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
    public void wellKnownKustoEndpoints_NationalClustersTest() throws KustoClientInvalidConnectionStringException, URISyntaxException {
        for (String c : new String[] {
                String.format("https://kustozszokb5yrauyq.kusto.chinacloudapi.cn,%s", chinaCloudLogin),
                "https://kustofrbwrznltavls.kusto.usgovcloudapi.net,https://login.microsoftonline.us",
                "https://kusto7j53clqswr4he.kusto.core.eaglex.ic.gov,https://login.microsoftonline.eaglex.ic.gov",
                "https://rpe2e0422132101fct2.kusto.core.microsoft.scloud,https://login.microsoftonline.microsoft.scloud",
                String.format("https://kustozszokb5yrauyq.kusto.chinacloudapi.cn,%s", chinaCloudLogin),
                "https://kustofrbwrznltavls.kusto.usgovcloudapi.net,https://login.microsoftonline.us",
                "https://kusto7j53clqswr4he.kusto.core.eaglex.ic.gov,https://login.microsoftonline.eaglex.ic.gov",
                "https://rpe2e0422132101fct2.kusto.core.microsoft.scloud,https://login.microsoftonline.microsoft.scloud",
        }) {
            String[] clusterAndLoginEndpoint = c.split(",");
            validateEndpoint(clusterAndLoginEndpoint[0], clusterAndLoginEndpoint[1]);
            // Test case sensitivity
            validateEndpoint(clusterAndLoginEndpoint[0].toUpperCase(), clusterAndLoginEndpoint[1].toUpperCase());
        }
    }

    @Test
    public void wellKnownKustoEndpoints_ProxyTest() throws KustoClientInvalidConnectionStringException, URISyntaxException {
        for (String c : new String[] {
                String.format("https://kusto.aria.microsoft.com,%s", DEFAULT_PUBLIC_LOGIN_URL),
                String.format("https://ade.loganalytics.io,%s", DEFAULT_PUBLIC_LOGIN_URL),
                String.format("https://ade.applicationinsights.io,%s", DEFAULT_PUBLIC_LOGIN_URL),
                String.format("https://kusto.aria.microsoft.com,%s", DEFAULT_PUBLIC_LOGIN_URL),
                String.format("https://adx.monitor.azure.com,%s", DEFAULT_PUBLIC_LOGIN_URL),
                String.format("https://cluster.playfab.com,%s", DEFAULT_PUBLIC_LOGIN_URL),
                String.format("https://cluster.playfabapi.com,%s", DEFAULT_PUBLIC_LOGIN_URL),
                String.format("https://cluster.playfab.cn,%s", chinaCloudLogin),
        }) {
            String[] clusterAndLoginEndpoint = c.split(",");
            validateEndpoint(clusterAndLoginEndpoint[0], clusterAndLoginEndpoint[1]);
            // Test case sensitivity
            validateEndpoint(clusterAndLoginEndpoint[0].toUpperCase(), clusterAndLoginEndpoint[1].toUpperCase());
        }
    }

    @Test
    public void wellKnownKustoEndpoints_ProxyNegativeTest() throws KustoClientInvalidConnectionStringException, URISyntaxException {
        for (String clusterName : new String[] {
                "https://cluster.kusto.aria.microsoft.com",
                "https://cluster.eu.kusto.aria.microsoft.com",
                "https://cluster.ade.loganalytics.io",
                "https://cluster.ade.applicationinsights.io",
                "https://cluster.adx.monitor.azure.com",
                "https://cluster.adx.applicationinsights.azure.cn",
                "https://cluster.adx.monitor.azure.eaglex.ic.gov"
        }) {
            {
                checkEndpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL, true);
            }
        }
    }

    @Test
    public void wellKnownKustoEndpoints_NegativeTest() throws KustoClientInvalidConnectionStringException, URISyntaxException {
        for (String clusterName : new String[] {
                "https://localhostess",
                "https://127.0.0.1.a",
                "https://some.azurewebsites.net",
                "https://kusto.azurewebsites.net",
                "https://test.kusto.core.microsoft.scloud",
                "https://cluster.kusto.azuresynapse.azure.cn"

        }) {
            {
                checkEndpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL, true);
            }
        }
    }

    @Test
    public void wellKnownKustoEndpoints_OverrideTest() throws KustoClientInvalidConnectionStringException, URISyntaxException {
        try {
            KustoTrustedEndpoints.setOverridePolicy(hostname -> true);
            checkEndpoint("https://kusto.kusto.windows.net", "", false);
            checkEndpoint("https://bing.com", "", false);

            KustoTrustedEndpoints.setOverridePolicy(hostname -> false);
            checkEndpoint("https://kusto.kusto.windows.net", "", true);
            checkEndpoint("https://bing.com", "", true);

            KustoTrustedEndpoints.setOverridePolicy(null);
            checkEndpoint("https://kusto.kusto.windows.net", DEFAULT_PUBLIC_LOGIN_URL, false);
            checkEndpoint("https://bing.com", DEFAULT_PUBLIC_LOGIN_URL, true);
        } finally {
            KustoTrustedEndpoints.setOverridePolicy(null);
        }
    }

    @Test
    public void wellKnownKustoEndpoints_AdditionalWebsites() throws KustoClientInvalidConnectionStringException, URISyntaxException {
        KustoTrustedEndpoints.addTrustedHosts(Arrays.asList(new MatchRule[] {
                new MatchRule(".someotherdomain1.net", false),
        }.clone()), true);

        // 2nd call - to validate that addition works
        KustoTrustedEndpoints.addTrustedHosts(Arrays.asList(new MatchRule[] {
                new MatchRule("www.someotherdomain2.net", true),
        }.clone()), false);

        for (String clusterName : new String[] {
                "https://some.someotherdomain1.net",
                "https://www.someotherdomain2.net",
        }) {
            {
                checkEndpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL, false);
            }
        }

        for (String clusterName : new String[] {
                "https://some.someotherdomain2.net",
        }) {
            {
                checkEndpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL, true);
            }
        }

        // Reset additional hosts
        KustoTrustedEndpoints.addTrustedHosts(null, true);
        // Validate that hosts are not allowed anymore
        for (String clusterName : new String[] {
                "https://some.someotherdomain1.net",
                "https://www.someotherdomain2.net",
        }) {
            {
                checkEndpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL, true);
            }
        }
    }

    private void checkEndpoint(String clusterName, String defaultPublicLoginUrl, boolean expectFail)
            throws KustoClientInvalidConnectionStringException, URISyntaxException {
        if (expectFail) {
            Assertions.assertThrows(KustoClientInvalidConnectionStringException.class, () -> validateEndpoint(clusterName,
                    defaultPublicLoginUrl));
        } else {
            validateEndpoint(clusterName, defaultPublicLoginUrl);
        }
    }

    private void validateEndpoint(String address, String loginEndpoint) throws URISyntaxException, KustoClientInvalidConnectionStringException {
        KustoTrustedEndpoints.validateTrustedEndpoint(new URI(address), loginEndpoint);
    }
}
