package com.microsoft.azure.kusto.data.auth.endpoints;

import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FastSuffixMatcher {
    private final int suffixLength;
    private final Map<String, List<MatchRule>> m_rules;

    /**
     * Creates a new matcher with the provided matching rules.
     *
     * @param rules - One or more matching rules to apply when Match
     *              is called
     * @return FastSuffixMatcher
     */
    public static FastSuffixMatcher create(List<MatchRule> rules) {
        Ensure.argIsNotNull(rules, "rules");
        int minRuleLength = rules.stream().min(Comparator.comparing(MatchRule::getSuffixLength))
                .map(MatchRule::getSuffixLength).orElse(0);

        Ensure.isTrue(minRuleLength > 0 && minRuleLength != Integer.MAX_VALUE, "Cannot have a match rule " +
                "whose length is zero");

        Map<String, List<MatchRule>> processedRules = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (MatchRule rule : rules) {
            String suffix = StringUtils.GetStringTail(rule.suffix, minRuleLength);
            List<MatchRule> list = processedRules.computeIfAbsent(suffix, k -> new ArrayList<>());
            list.add(rule.Clone());
        }

        return new FastSuffixMatcher(minRuleLength, processedRules);
    }

    private FastSuffixMatcher(int suffixLength, Map<String, List<MatchRule>> rules) {
        this.suffixLength = suffixLength;
        m_rules = rules;
    }

    /**
     * Creates a new matcher with the provided matching rules.
     *
     * @param existing - An existing matcher whose rules are to be baseline Match
     * @param rules    - One or more matching rules to apply when Match
     *                 is called
     * @return FastSuffixMatcher
     */
    public static FastSuffixMatcher create(FastSuffixMatcher existing, List<MatchRule> rules) {
        if (existing == null || existing.m_rules.size() == 0) {
            return create(rules);
        }

        if (rules == null || rules.isEmpty()) {
            return existing;
        }

        List<MatchRule> list = Stream.concat(rules.stream(),
                existing.m_rules.values().stream().flatMap(Collection::stream))
                .collect(Collectors.toList());
        return create(list);
    }

    /**
     * @param candidate - A string to match to the list of match rules
     * @return true if at least one of the rules matched.
     */
    public Boolean isMatch(String candidate) {
        return match(candidate).isMatch;
    }

    public MatchResult match(String candidate) {
        Ensure.argIsNotNull(candidate, "candidate");

        if (candidate.length() < suffixLength) {
            return new MatchResult(false, null);
        }

        List<MatchRule> matchRules = m_rules.get(StringUtils.GetStringTail(candidate, suffixLength));
        if (matchRules != null) {
            for (MatchRule rule : matchRules) {
                if (StringUtils.endsWithIgnoreCase(candidate, rule.suffix)) {
                    if (candidate.length() == rule.suffix.length()
                            || !rule.exact) {
                        return new MatchResult(true, rule);
                    }
                }
            }
        }

        return new MatchResult(false, null);
    }
}
