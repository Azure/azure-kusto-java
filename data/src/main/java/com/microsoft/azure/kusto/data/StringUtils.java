package com.microsoft.azure.kusto.data;



public class StringUtils {
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

    public static String normalizeEntityName(String name) {
        if (Utils.isNullOrEmpty(name)) {
            return name;
        } else if (name.startsWith("[")) {
            return name;
        } else if (!name.contains("'")) {
            return "['" + name + "']";
        } else {
            return "[\"" + name + "\"]";
        }
    }
}
