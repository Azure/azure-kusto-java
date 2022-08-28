package com.microsoft.azure.kusto.data;

public class StringUtils extends org.apache.commons.lang3.StringUtils {
    private StringUtils() {
        // Hide constructor for static class
    }

    public static String getStringTail(String val, int minRuleLength) {
        if (minRuleLength <= 0) {
            return "";
        }

        if (minRuleLength >= val.length()) {
            return val;
        }

        return val.substring(val.length() - minRuleLength);
    }

    public static String quoteEntityName(String name) {
        return "['" + name + "']";
    }
}
