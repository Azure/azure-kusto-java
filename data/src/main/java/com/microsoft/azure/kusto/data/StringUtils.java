package com.microsoft.azure.kusto.data;

public class StringUtils {
    public static String GetStringTail(String val, int minRuleLength) {
        if (minRuleLength <= 0) {
            return "";
        }

        if (minRuleLength >= val.length()) {
            return val;
        }

        return val.substring(val.length() - minRuleLength);
    }

    private StringUtils() {
    }
}
