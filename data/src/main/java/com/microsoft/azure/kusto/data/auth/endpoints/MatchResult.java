package com.microsoft.azure.kusto.data.auth.endpoints;

public class MatchResult {
    Boolean isMatch;
    MatchRule matcher;

    public MatchResult(Boolean isMatch, MatchRule matcher) {
        this.isMatch = isMatch;
        this.matcher = matcher;
    }
}
