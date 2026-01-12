// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.auth.endpoints

/**
 * Represents a matching rule for endpoint validation.
 * @param suffix The suffix or hostname to match
 * @param exact If true, the candidate must exactly match the suffix. If false, candidate must end with the suffix.
 */
data class MatchRule(
    val suffix: String,
    val exact: Boolean,
) {
    val suffixLength: Int
        get() = suffix.length
}

/**
 * Result of a match operation.
 * @param isMatch Whether the candidate matched
 * @param matchedRule The rule that matched, or null if no match
 */
data class MatchResult(
    val isMatch: Boolean,
    val matchedRule: MatchRule?,
)

/**
 * A fast suffix matcher that efficiently matches hostnames against a set of rules.
 * Uses a map indexed by suffix tail for O(1) lookup.
 */
class FastSuffixMatcher private constructor(
    private val suffixLength: Int,
    private val rules: Map<String, List<MatchRule>>,
) {
    companion object {
        /**
         * Creates a new matcher with the provided matching rules.
         * @param rules One or more matching rules to apply when match is called
         * @return FastSuffixMatcher
         */
        fun create(rules: List<MatchRule>): FastSuffixMatcher {
            require(rules.isNotEmpty()) { "Rules cannot be empty" }

            val minRuleLength = rules.minOfOrNull { it.suffixLength } ?: 0
            require(minRuleLength > 0) {
                "Cannot have a match rule whose length is zero"
            }

            val processedRules = mutableMapOf<String, MutableList<MatchRule>>()
            for (rule in rules) {
                val suffix = rule.suffix.takeLast(minRuleLength).lowercase()
                processedRules.getOrPut(suffix) { mutableListOf() }.add(rule.copy())
            }

            return FastSuffixMatcher(minRuleLength, processedRules)
        }

        /**
         * Creates a new matcher with the provided matching rules, extending an
         * existing matcher.
         * @param existing An existing matcher whose rules are to be baseline
         * @param rules One or more matching rules to apply when match is called
         * @return FastSuffixMatcher
         */
        fun create(
            existing: FastSuffixMatcher?,
            rules: List<MatchRule>,
        ): FastSuffixMatcher {
            if (existing == null || existing.rules.isEmpty()) {
                return create(rules)
            }

            if (rules.isEmpty()) {
                return existing
            }

            val combinedRules =
                rules + existing.rules.values.flatten()
            return create(combinedRules)
        }
    }

    /**
     * Checks if a candidate string matches any of the rules.
     * @param candidate A string to match to the list of match rules
     * @return true if at least one of the rules matched
     */
    fun isMatch(candidate: String): Boolean = match(candidate).isMatch

    /**
     * Matches an input string to the list of match rules.
     * @param candidate A string to match
     * @return MatchResult with match status and the matched rule if any
     */
    fun match(candidate: String): MatchResult {
        if (candidate.length < suffixLength) {
            return MatchResult(false, null)
        }

        val tail = candidate.takeLast(suffixLength).lowercase()
        val matchRules = rules[tail]

        if (matchRules != null) {
            for (rule in matchRules) {
                if (candidate.endsWith(rule.suffix, ignoreCase = true)) {
                    if (candidate.length == rule.suffix.length || !rule.exact) {
                        return MatchResult(true, rule)
                    }
                }
            }
        }

        return MatchResult(false, null)
    }
}