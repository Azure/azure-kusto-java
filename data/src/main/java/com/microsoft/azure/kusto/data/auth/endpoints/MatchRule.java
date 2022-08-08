package com.microsoft.azure.kusto.data.auth.endpoints;

public class MatchRule
{
    /// <summary>
    /// The suffix which the candidate must end with in order to match.
    /// </summary>
    public final String suffix;

    /// <summary>
    /// Indicates whether the match must be exact (the candidate must
    /// not have any prefix) or not.
    /// </summary>
    public final Boolean exact;

    public int getSuffixLength() {
        return suffix == null ? 0 : suffix.length();
    }

    public MatchRule(String suffix, Boolean exact)
    {
        this.suffix = suffix;
        this.exact = exact;
    }

    /// <summary>
    /// Clones this object.
    /// </summary>
    public MatchRule Clone()
    {
        return new MatchRule(suffix, exact);
    }
}
