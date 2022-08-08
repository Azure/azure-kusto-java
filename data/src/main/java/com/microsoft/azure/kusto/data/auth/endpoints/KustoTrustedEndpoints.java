package com.microsoft.azure.kusto.data.auth.endpoints;

import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoClientInvalidConnectionStringException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Predicate;

/**
 * A helper class to determine which DNS names are "well-known/trusted"'
 * Kusto endpoints. Untrusted endpoints might require additional configuration
 * before they can be used, for security reasons.
 */
public class KustoTrustedEndpoints {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static boolean enableWellKnownKustoEndpointsValidation = true;
    private static final Map<String, FastSuffixMatcher> matchers;
    private static FastSuffixMatcher additionalMatcher;
    private static Predicate<String> overrideMatcher; // We could unify this with matchers, but separating makes debugging easier

    static {
        matchers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        WellKnownKustoEndpointsData.getInstance().AllowedEndpointsByLogin.forEach((key, value) -> {
            List<MatchRule> rules = new ArrayList<>();
            value.AllowedKustoSuffixes.forEach(suffix -> rules.add(new MatchRule(suffix, false)));
            value.AllowedKustoHostnames.forEach(hostname -> rules.add(new MatchRule(hostname, true)));
            matchers.put(key, FastSuffixMatcher.Create(rules));
        });

        additionalMatcher = null;
        overrideMatcher = null;
    }

    /**
     * @param matcher Rules that determine if a hostname is a valid/trusted Kusto endpoint
     *  (replaces existing rules). Null means "use the default policy".
     */
    public static void SetOverridePolicy(Predicate<String> matcher) {
        overrideMatcher = matcher;
    }

    /**
     * Throw an exception if the endpoint specified is not trusted.
     *
     * @param uri - Kusto endpoint
     * @param loginEndpoint The login endpoint to check against.
     * @throws KustoClientInvalidConnectionStringException - Endpoint is not a trusted Kusto endpoint
     */
    public static void ValidateTrustedEndpoint(String uri, String loginEndpoint) throws KustoClientInvalidConnectionStringException {
        try {
            ValidateTrustedEndpoint(new URI(uri), loginEndpoint);
        } catch (URISyntaxException ex) {
            throw new KustoClientInvalidConnectionStringException(ex);
        }
    }

    /**
     * Throw an exception if the endpoint specified is not trusted.
     *
     * @param uri - Kusto endpoint
     * @throws KustoClientInvalidConnectionStringException - Endpoint is not a trusted Kusto endpoint
     */
    public static void ValidateTrustedEndpoint(String uri) throws KustoClientInvalidConnectionStringException {
        try {
            ValidateTrustedEndpoint(new URI(uri), CloudInfo.retrieveCloudInfoForCluster(uri).getLoginEndpoint());
        } catch (URISyntaxException | DataServiceException ex) {
            throw new KustoClientInvalidConnectionStringException(ex);
        }
    }

    /**
     * Is the endpoint uri trusted?
     *
     * @param uri The endpoint to inspect.
     * @param loginEndpoint The login endpoint to check against.
     * @throws KustoClientInvalidConnectionStringException - Endpoint is not a trusted Kusto endpoint
     */
    public static void ValidateTrustedEndpoint(URI uri, String loginEndpoint) throws KustoClientInvalidConnectionStringException {
        Ensure.argIsNotNull(uri, "uri");

        // Check that target hostname is trusted and can accept security token
        ValidateHostnameIsTrusted(uri.getHost(), loginEndpoint);
    }

    /**
     * Adds the rules that determine if a hostname is a valid/trusted Kusto endpoint
     * (extends existing rules).
     *
     * @param rules   - A set of rules
     * @param replace - If true nullifies the last added rules
     */
    public synchronized static void AddTrustedHosts(List<MatchRule> rules, boolean replace) {
        if (replace) {
            additionalMatcher = null;
        }

        if (rules == null || rules.isEmpty()) {
            return;
        }

        additionalMatcher = FastSuffixMatcher.Create(additionalMatcher, rules);
    }

    private static void ValidateHostnameIsTrusted(String hostname, String loginEndpoint) throws KustoClientInvalidConnectionStringException {
        // The loopback is unconditionally allowed (since we trust ourselves)
        if (CloudInfo.isLocalAddress(hostname)) {
            return;
        }

        // Either check the override matcher OR the matcher:
        if (overrideMatcher != null) {
            if (overrideMatcher.test(hostname)) {
                return;
            }
        } else {
            FastSuffixMatcher matcher = matchers.get(loginEndpoint);
            if (matcher != null && matcher.isMatch(hostname)) {
                return;
            }
        }

        if (additionalMatcher != null && additionalMatcher.isMatch(hostname)) {
            return;
        }

        if (!enableWellKnownKustoEndpointsValidation) {
            log.warn("Can't communicate with '{}' as this hostname is currently not trusted; please see https://aka.ms/kustotrustedendpoints.", hostname);
            return;
        }

        throw new KustoClientInvalidConnectionStringException(
                String.format(
                        "$$ALERT[ValidateHostnameIsTrusted]: Can't communicate with '%s' as this hostname is currently not trusted; please see https://aka.ms/kustotrustedendpoints",
                        hostname));
    }
}
