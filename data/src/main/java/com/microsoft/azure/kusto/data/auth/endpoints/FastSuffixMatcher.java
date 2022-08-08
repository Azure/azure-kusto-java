package com.microsoft.azure.kusto.data.auth.endpoints;

import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.StringUtils;

import java.util.*;

public class FastSuffixMatcher {
    private int suffixLength;
    private Map<String, List<MatchRule>> m_rules;

    /**
     * Creates a new matcher with the provided matching rules.
     * @param rules - One or more matching rules to apply when <see cref="Match(string)"/>
     * is called
     */
    public static FastSuffixMatcher Create(List<MatchRule> rules)
    {
        Ensure.argIsNotNull(rules, "rules");
        int minRuleLength = rules.stream().min(Comparator.comparing(MatchRule::getSuffixLength))
                .map(MatchRule::getSuffixLength).orElse(0);

        Ensure.isTrue(minRuleLength > 0 && minRuleLength != Integer.MAX_VALUE, "Cannot have a match rule " +
                "whose length is zero");

        Map<String, List<MatchRule>> processedRules = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (MatchRule rule : rules)
        {
            String suffix = StringUtils.GetStringTail(rule.suffix, minRuleLength);
            List<MatchRule> list = processedRules.computeIfAbsent(suffix, k -> new ArrayList<>());
            list.add(rule.Clone());
        }

        return new FastSuffixMatcher(minRuleLength, processedRules);
    }

    private FastSuffixMatcher(int suffixLength, Map<String, List<MatchRule>> rules)
    {
        this.suffixLength = suffixLength;
        m_rules = rules;
    }
    /**
     * Creates a new matcher with the provided matching rules.
     * @param existing - An existing matcher whose rules are to be baseline <see cref="Match(string)"/>
     * @param rules - One or more matching rules to apply when <see cref="Match(string)"/>
     * is called
     */
    /// <param name="existing">An existing matcher whose rules are to be baseline.</param>
    /// <param name="rules">One or more matching rules to apply when <see cref="Match(string)"/>
    ///   is called.</param>
    public static FastSuffixMatcher Create(FastSuffixMatcher existing, List<MatchRule> rules)
    {
        if (existing == null || existing.m_rules.size() == 0)
        {
            return Create(rules);
        }

        if (rules == null || rules.isEmpty()) {
            return existing;
        }

        List<MatchRule> list = new ArrayList<>();
        for (Map.Entry<String,List<MatchRule>> entry: existing.m_rules.entrySet()) {
            list.addAll(entry.getValue());
        }

        list.addAll(rules);
        return Create(list);
    }

    /**
     * Matches an input string to the list of match rules, and returns true
     * if at least one of the rules matched.
     */

    /// </summary>
    public Boolean isMatch(String candidate)
    {
        return Match(candidate).isMatch;
    }

    public MatchResult Match(String candidate)
    {
        Ensure.argIsNotNull(candidate, "candidate");

        if (candidate.length() >= suffixLength){
            List<MatchRule> matchRules = m_rules.getOrDefault(StringUtils.GetStringTail(candidate, suffixLength),
                    new ArrayList<>());
            for ( MatchRule rule : matchRules)
            {
                if (StringUtils.endsWithIgnoreCase(candidate, rule.suffix))
                {
                    if (candidate.length() == rule.suffix.length()
                            || !rule.exact)
                    {
                        return new MatchResult(true, rule);
                    }
                }
            }
        }

        return new MatchResult(false, null);
    }
}
