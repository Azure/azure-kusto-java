package com.microsoft.azure.kusto.data;

public class StringUtils
{
    public static boolean startsWithIgnoreCase(String str, String prefix)
    {
        return str.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    public static boolean endsWithIgnoreCase(String str, String suffix)
    {
        int suffixLength = suffix.length();
        return str.regionMatches(true, str.length() - suffixLength, suffix, 0, suffixLength);
    }

    public static String GetStringTail(String val, int minRuleLength) {
        if (minRuleLength <= 0)
        {
            return "";
        }

        if (minRuleLength >= val.length())
        {
            return val;
        }

        return val.substring(val.length() - minRuleLength);
    }

    private StringUtils()
    {
    }
}