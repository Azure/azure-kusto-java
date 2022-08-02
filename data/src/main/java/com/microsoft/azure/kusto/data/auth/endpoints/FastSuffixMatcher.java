package com.microsoft.azure.kusto.data.auth.endpoints;

import com.microsoft.azure.kusto.data.Ensure;

import java.util.*;

public class FastSuffixMatcher {
    private int suffixLength;
    private HashMap<String, List<MatchRule>> m_rules;
    private Boolean m_ignoreCase;

    /**
     * Creates a new matcher with the provided matching rules.
     * @param rules - One or more matching rules to apply when <see cref="Match(string)"/>
     * is called
     * @param ignoreCase - Whether to ignore the casing or not (default: no)
     */
    public static FastSuffixMatcher Create(List<MatchRule> rules)
    {
        Ensure.argIsNotNull(rules, "rules");
        int minRuleLength = rules.stream().min(Comparator.comparing(MatchRule::getSuffixLength))
                .map(MatchRule::getSuffixLength).orElse(0);

        Ensure.isTrue(minRuleLength > 0 && minRuleLength != Integer.MAX_VALUE, "Cannot have a match rule " +
                "whose length is zero");

        HashMap<String, List<MatchRule>> processedRules = new HashMap<>();
        for (MatchRule rule : rules)
        {
            String suffix = rule.GetSuffixTail(minRuleLength);
            List<MatchRule> list = processedRules.computeIfAbsent(suffix, k -> new ArrayList<>());
            list.add(rule.Clone());
        }

        return new FastSuffixMatcher(minRuleLength, processedRules);
    }

    /// <summary>
    /// Creates a new matcher with the provided matching rules based on an existing matcher.
    /// </summary>
    /// <param name="existing">An existing matcher whose rules are to be baseline.</param>
    /// <param name="rules">One or more matching rules to apply when <see cref="Match(string)"/>
    ///   is called.</param>
    /// <param name="ignoreCase">Whether to ignore the casing or not (default: no)</param>
    public static FastSuffixMatcher Create(FastSuffixMatcher existing, IEnumerable<MatchRule> rules, bool ignoreCase = false)
    {
        if (existing == null || existing.m_rules.Count == 0)
        {
            return Create(rules, ignoreCase);
        }

        if (rules.SafeFastNone() && existing.m_ignoreCase == ignoreCase)
        {
            return existing;
        }

        List<MatchRule> list = new List<MatchRule>();
        foreach (var item in existing.m_rules)
        {
            list.AddRange(item.Value);
        }
        if (rules != null)
        {
            foreach (var rule in rules)
            {
                list.Add(rule);
            }
        }
        return Create(list, ignoreCase);
    }

    private FastSuffixMatcher(int suffixLength, Dictionary<string, List<MatchRule>> rules, bool ignoreCase)
    {
        m_suffixLength = suffixLength;
        m_rules = rules;
        m_ignoreCase = ignoreCase;
    }
        #endregion

        #region Public API
    /// <summary>
    /// Matches an input string to the list of match rules, and returns true
    /// if at least one of the rules matched.
    /// </summary>
    public bool IsMatch(string candidate)
    {
        return Match(candidate).IsMatch;
    }
    public MatchResult Match(String candidate)
    {
        Ensure.argIsNotNull(candidate, "candidate");

        if (candidate.Length >= m_suffixLength
                && m_rules.TryGetValue(candidate.SafeGetTail(m_suffixLength), out var list))
        {
            foreach (var rule in list)
            {
                if (candidate.EndsWith(rule.Suffix, comparisonType))
                {
                    if (candidate.Length == rule.Suffix.Length
                            || !rule.Exact)
                    {
                        return (true, rule);
                    }
                }
            }
        }

        return (false, null);
    }
}
