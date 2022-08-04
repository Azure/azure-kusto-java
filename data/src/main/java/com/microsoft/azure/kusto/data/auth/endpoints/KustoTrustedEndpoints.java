package com.microsoft.azure.kusto.data.auth.endpoints;

import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.exceptions.KustoClientInvalidConnectionStringException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/// <summary>
/// A helper class to determine which DNS names are "well-known/trusted"
/// Kusto endpoints. Untrusted endpoints might require additional configuration
/// before they can be used, for security reasons.
/// </summary>
public class KustoTrustedEndpoints {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static boolean enableWellKnownKustoEndpointsValidation = true;
    private static FastSuffixMatcher matcher;
    private static FastSuffixMatcher additionalMatcher;
    private static Predicate<String> overrideMatcher; // We could unify this with s_matcher, but separating makes debugging easier

    KustoTrustedEndpoints()
    {
        List<MatchRule> rules = new ArrayList<>();

//        WellKnownKustoEndpointsData.getInstance().AllowedKustoSuffixes.forEach(suffix -> rules.add(new MatchRule(suffix, false)));
//        WellKnownKustoEndpointsData.getInstance().AllowedKustoHostnames.forEach(hostname -> rules.add(new MatchRule(hostname, true)));

        WellKnownKustoEndpointsData.getInstance().AllowedKustoSuffixes.forEach(pair -> )
        matcher = FastSuffixMatcher.Create(rules);
        additionalMatcher = null;
        overrideMatcher = null;
    }

    /**
     * Sets the rules that determine if a hostname is a valid/trusted Kusto endpoint
     *  (replaces existing rules). Null means "use the default policy".
     */
    public static void SetOverridePolicy(Predicate<String> matcher)
    {
        overrideMatcher = matcher;
    }

    /// <summary>
    /// Throw an exception if the endpoint specified in the <paramref name="uri"/>
    /// is not trusted.
    /// </summary>
    /// <exception cref="KustoClientInvalidConnectionStringException"></exception>
    public static void ValidateTrustedEndpoint(String uri) throws KustoClientInvalidConnectionStringException {
        try {
            ValidateTrustedEndpoint(new URI(uri));
        } catch (URISyntaxException ex) {
            throw new KustoClientInvalidConnectionStringException(ex);
        }
    }

    /// <summary>
    /// Is the endpoint specified in the <paramref name="uri"/> trusted?
    /// </summary>
    /// <param name="uri">The endpoint to inspect.</param>
    /// <param name="errorMessage">If this is not a trusted endpoint, holds a display
    /// string that describes the "error".</param>
    /// <returns>True if this is a trusted endpoint, false otherwise.</returns>
    public static void ValidateTrustedEndpoint(URI uri) throws KustoClientInvalidConnectionStringException {
        Ensure.argIsNotNull(uri, "uri");

        // Check that target hostname is trusted and can accept security token
        ValidateHostnameIsTrusted(uri.getHost());
    }

    /// <summary>
    /// Adds the rules that determine if a hostname is a valid/trusted Kusto endpoint
    /// (extends existing rules).
    /// </summary>
    public synchronized static void AddTrustedHosts(List<MatchRule> rules, boolean replace)
    {
        if (replace)
        {
            additionalMatcher = null;
        }

        if (rules.isEmpty())
        {
            return;
        }

        additionalMatcher = FastSuffixMatcher.Create(additionalMatcher, rules);
    }

    private static void ValidateHostnameIsTrusted(String hostname) throws KustoClientInvalidConnectionStringException {
        // The loopback is unconditionally allowed (since we trust ourselves)
        if (hostname.equals(CloudInfo.LOCALHOST) || hostname.equals(CloudInfo.LOCALHOST_IP))
        {
            return;
        }

        // Either check the override matcher OR the matcher:
        if (overrideMatcher != null)
        {
            if (overrideMatcher.test(hostname))
            {
                return;
            }
        }
        else if (matcher.isMatch(hostname))
        {
            return;
        }

        if (additionalMatcher != null && additionalMatcher.isMatch(hostname))
        {
            return;
        }


        if (!enableWellKnownKustoEndpointsValidation) {
            log.warn("Can't communicate with '{}' as this hostname is currently not trusted; please see https://aka.ms/kustotrustedendpoints.", hostname);
            return;
        }

        throw new KustoClientInvalidConnectionStringException(
                String.format("$$ALERT[ValidateHostnameIsTrusted]: Can't communicate with '%s' as this hostname is currently not trusted; please see https://aka.ms/kustotrustedendpoints", hostname));
    }

    public static void ValidateTrustedLogin(String loginEndpoit) {

    }
}