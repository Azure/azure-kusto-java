//package com.microsoft.azure.kusto.data.auth;
//
///// <summary>
///// A helper class to determine which DNS names are "well-known/trusted"
///// Kusto endpoints. Untrusted endpoints might require additional configuration
///// before they can be used, for security reasons.
///// </summary>
//public class KustoTrustedEndpoints {
//    public static boolean enableWellKnownKustoEndpointsValidation = true;
//    private readonly static object s_lock = new object();
//    private static FastSuffixMatcher s_matcher;
//    private static FastSuffixMatcher s_additionalMatcher;
//    private static Predicate<string> s_overrideMatcher; // We could unify this with s_matcher, but separating makes debugging easier
//
//    static KustoTrustedEndpoints()
//    {
//        var rules = new List<FastSuffixMatcher.MatchRule>();
//
//        Impl.com.microsoft.azure.kusto.data.auth.endpoints.WellKnownKustoEndpointsData.Instance.AllowedKustoSuffixes.ForEach(suffix => rules.Add(new FastSuffixMatcher.MatchRule(suffix, exact: false)));
//        Impl.com.microsoft.azure.kusto.data.auth.endpoints.WellKnownKustoEndpointsData.Instance.AllowedKustoHostnames.ForEach(hostname => rules.Add(new FastSuffixMatcher.MatchRule(hostname, exact: true)));
//        s_matcher = FastSuffixMatcher.Create(rules, ignoreCase: true);
//        s_additionalMatcher = null;
//        s_overrideMatcher = null;
//    }
//        #endregion
//
//        #region Public API
//    /// <summary>
//    /// Sets the rules that determine if a hostname is a valid/trusted Kusto endpoint
//    /// (replaces existing rules). Null means "use the default policy".
//    /// </summary>
//    public static void SetOverridePolicy(Predicate<string> matcher)
//    {
//        s_overrideMatcher = matcher;
//    }
//
//    /// <summary>
//    /// Throw an exception if the endpoint specified in the <paramref name="kcsb"/>
//    /// is not trusted.
//    /// </summary>
//    /// <exception cref="KustoClientInvalidConnectionStringException"></exception>
//    public static void ValidateTrustedEndpoint(KustoConnectionStringBuilder kcsb)
//    {
//        if (!IsTrustedEndpoint(kcsb, out string errorMessage))
//        {
//            throw new KustoClientInvalidConnectionStringException(errorMessage);
//        }
//    }
//
//    /// <summary>
//    /// Throw an exception if the endpoint specified in the <paramref name="uri"/>
//    /// is not trusted.
//    /// </summary>
//    /// <exception cref="KustoClientInvalidConnectionStringException"></exception>
//    public static void ValidateTrustedEndpoint(Uri uri)
//    {
//        if (!IsTrustedEndpoint(uri, out string errorMessage))
//        {
//            throw new KustoClientInvalidConnectionStringException(errorMessage);
//        }
//    }
//
//    /// <summary>
//    /// Is the endpoint specified in the <paramref name="kcsb"/> trusted?
//    /// </summary>
//    /// <param name="kcsb">The endpoint to inspect.</param>
//    /// <param name="errorMessage">If this is not a trusted endpoint, holds a display
//    /// string that describes the "error".</param>
//    /// <returns>True if this is a trusted endpoint, false otherwise.</returns>
//    public static bool IsTrustedEndpoint(KustoConnectionStringBuilder kcsb, out string errorMessage)
//    {
//        Ensure.ArgIsNotNull(kcsb, nameof(kcsb));
//        errorMessage = null;
//
//        // Check that target hostname is trusted and can accept security token
//        return ValidateHostnameIsTrusted(kcsb.ConnectionScheme, kcsb.Hostname, ref errorMessage);
//    }
//
//    /// <summary>
//    /// Is the endpoint specified in the <paramref name="uri"/> trusted?
//    /// </summary>
//    /// <param name="uri">The endpoint to inspect.</param>
//    /// <param name="errorMessage">If this is not a trusted endpoint, holds a display
//    /// string that describes the "error".</param>
//    /// <returns>True if this is a trusted endpoint, false otherwise.</returns>
//    public static bool IsTrustedEndpoint(Uri uri, out string errorMessage)
//    {
//        Ensure.ArgIsNotNull(uri, nameof(uri));
//        errorMessage = null;
//
//        // Check that target hostname is trusted and can accept security token
//        return ValidateHostnameIsTrusted(uri.Scheme, uri.Host, ref errorMessage);
//    }
//
//    /// <summary>
//    /// Adds the rules that determine if a hostname is a valid/trusted Kusto endpoint
//    /// (extends existing rules).
//    /// </summary>
//    public static void AddTrustedHosts(IEnumerable<FastSuffixMatcher.MatchRule> rules, bool replace = false)
//    {
//        lock (s_lock)
//        {
//            if (replace)
//            {
//                s_additionalMatcher = null;
//            }
//
//            if (rules.SafeFastNone())
//            {
//                return;
//            }
//
//            s_additionalMatcher = FastSuffixMatcher.Create(s_additionalMatcher, rules, ignoreCase: true);
//        }
//    }
//        #endregion
//
//        #region Private implementation
//    private static bool ValidateHostnameIsTrusted(string scheme, string hostname, ref string errorMessage)
//    {
//        // The loopback is unconditionally allowed (since we trust ourselves)
//        if (Kusto.Cloud.Platform.Communication.CommunicationModelUtils.IsLocalAddress(hostname))
//        {
//            return true;
//        }
//
//        // Either check the override matcher OR the matcher:
//        if (s_overrideMatcher != null)
//        {
//            if (s_overrideMatcher(hostname))
//            {
//                return true;
//            }
//        }
//        else if (s_matcher.IsMatch(hostname))
//        {
//            return true;
//        }
//
//        if (s_additionalMatcher?.IsMatch(hostname) ?? false)
//        {
//            return true;
//        }
//
//        if (s_enableWellKnownKustoEndpointsValidation.Value)
//        {
//            errorMessage = $"Can't communicate with '{hostname}' as this hostname is currently not trusted; please see https://aka.ms/kustotrustedendpoints.";
//            return false;
//        }
//        else
//        {
//            PrivateTracer.Tracer.TraceWarning("$$ALERT[ValidateHostnameIsTrusted]: Can't communicate with '{0}' as this hostname is currently not trusted; please see https://aka.ms/kustotrustedendpoints", hostname);
//            return true;
//        }
//    }
//        #endregion
//
//        #region class PrivateTracer
//    private class PrivateTracer : TraceSourceBase<PrivateTracer>
//    {
//        public override string Id => "KD.KustoTrustedEndpoints";
//        public override TraceVerbosity DefaultVerbosity => TraceVerbosity.Info;
//    }
//        #endregion
//}