package com.microsoft.azure.kusto.data;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class StringUtils {

    // Character constants for line endings (similar to Apache Commons Lang3 CharUtils)
    private static final String EMPTY = "";

    private StringUtils() {
        // Hide constructor for static class
    }

    public static @NotNull String getStringTail(String val, int minRuleLength) {
        if (minRuleLength <= 0) {
            return "";
        }

        if (minRuleLength >= val.length()) {
            return val;
        }

        return val.substring(val.length() - minRuleLength);
    }

    public static String normalizeEntityName(String name) {
        if (isBlank(name)) {
            return name;
        } else if (name.startsWith("[")) {
            return name;
        } else if (!name.contains("'")) {
            return "['" + name + "']";
        } else {
            return "[\"" + name + "\"]";
        }
    }

    /**
     * Checks if a CharSequence is empty (""), null or not.
     * @param cs The CharSequence to check, may be null
     * @return true if the CharSequence is empty or null
     */

    public static boolean isEmpty(CharSequence cs) {
        return isNull(cs) || cs.length() == 0;
    }

    private static boolean isNull(CharSequence cs) {
        return cs == null;
    }

    /**
     * Checks if a CharSequence is empty (""), null or whitespace only.
     * Whitespace is defined by {@link Character#isWhitespace(char)}.
     *
     * @param cs the CharSequence to check, may be null
     * @return true if the CharSequence is empty or null or whitespace only
     */
    public static boolean isBlank(CharSequence cs) {
        if (isEmpty(cs)) {
            return true;
        }
        for (int i = 0; i < cs.length(); i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if a CharSequence is not empty (""), not null and not whitespace only.
     * Whitespace is defined by {@link Character#isWhitespace(char)}.
     *
     * @param cs the CharSequence to check, may be null
     * @return true if the CharSequence is not empty and not null and not whitespace only
     */
    public static boolean isNotBlank(CharSequence cs) {
        return !isBlank(cs);
    }

    /**
     * Removes the last character from a string.
     *
     * @param str the string to chop
     * @return the string with the last character removed, or null if null string input
     */
    public static @Nullable String chop(final String str) {
        if (isNull(str)) {
            return null;
        }
        final int strLen = str.length();
        if (strLen <= 1) {
            return EMPTY;
        }

        final int lastIdx = strLen - 1;
        return str.substring(0, lastIdx);
    }

    /**
     * Removes a substring from the end of a string (case insensitive).
     *
     * @param str    the string to process
     * @param remove the substring to remove from the end
     * @return the string with the suffix removed
     */
    public static String removeEndIgnoreCase(String str, String remove) {
        if (isEmpty(str) || isEmpty(remove)) {
            return str;
        }
        if (endsWithIgnoreCase(str, remove)) {
            return str.substring(0, str.length() - remove.length());
        }
        return str;
    }

    /**
     * Prepends a prefix to a string if it doesn't already start with it.
     *
     * @param str    the string to process
     * @param prefix the prefix to prepend
     * @return the string with the prefix prepended if it wasn't already there
     */
    public static String prependIfMissing(String str, String prefix) {
        if (isNull(str) || isNull(prefix)) {
            return str;
        }
        if (!str.startsWith(prefix)) {
            return prefix + str;
        }
        return str;
    }

    /**
     * Appends the suffix to the end of the string if the string does not already end with any of the suffixes.
     *
     * @param str the string to process
     * @param suffix the suffix to append to the end of the string
     * @param suffixes additional suffixes that are valid terminators
     * @return a new String if suffix was appended, the same string otherwise
     */
    public static @Nullable String appendIfMissing(String str, CharSequence suffix, CharSequence... suffixes) {
        if (isNull(str) || isNull(suffix)) {
            return str;
        }

        // Check if string already ends with the main suffix
        if (str.endsWith(suffix.toString())) {
            return str;
        }
        // Check if string ends with any of the additional suffixes
        if (suffixes != null) {
            for (CharSequence s : suffixes) {
                if (s != null && str.endsWith(s.toString())) {
                    return str;
                }
            }
        }
        // String doesn't end with any of the suffixes, append the main suffix
        return str + suffix;
    }

    /**
     *
     * Case-insensitive check if a CharSequence ends with a specified suffix.
     * nulls are handled without exceptions. Two null references are considered to be equal.
     * The comparison is case-insensitive.
     *
     * @param str the CharSequence to check, may be null
     * @param suffix the suffix to find, may be null
     * @return true if the CharSequence ends with the suffix, case-insensitive, or both null
     */
    public static boolean endsWithIgnoreCase(CharSequence str, CharSequence suffix) {
        boolean areBothNull = isNull(str) && isNull(suffix);
        boolean isEitherNull = isNull(str) || isNull(suffix);
        if (isEitherNull) {
            return areBothNull;
        }
        if (suffix.length() > str.length()) {
            return false;
        }
        return str.toString().regionMatches(true, str.length() - suffix.length(), suffix.toString(), 0, suffix.length());
    }



    /**
     * Unescapes Java string escape sequences in the input string.
     * This method provides custom implementation to replace Apache Commons Text
     * StringEscapeUtils.unescapeJava() method with exactly the same behavior.
     * Supported escape sequences:
     * <ul>
     * <li>\\ -&gt; \</li>
     * <li>\" -&gt; "</li>
     * <li>\' -&gt; '</li>
     * <li>\n -&gt; newline</li>
     * <li>\r -&gt; carriage return</li>
     * <li>\t -&gt; tab</li>
     * <li>\b -&gt; backspace</li>
     * <li>\f -&gt; form feed</li>
     * <li>\XXXX -&gt; Unicode character (where XXXX is a 4-digit hex number)</li>
     * <li>\XXX -&gt; Octal character (where XXX is 1-3 octal digits)</li>
     * </ul>
     *
     * @param str the string to unescape, may be null
     * @return the unescaped string, or null if input was null
     */
    public static @Nullable String unescapeJava(String str) {
        if (isEmpty(str) || str.isEmpty()) {
            return str;
        }

        StringBuilder result = new StringBuilder(str.length());
        int length = str.length();

        for (int i = 0; i < length; i++) {
            char ch = str.charAt(i);

            if (ch != '\\') {
                result.append(ch);
                continue;
            }

            // Handle escape sequences
            if (i + 1 >= length) {
                // Trailing backslash - just append it
                result.append(ch);
                continue;
            }

            char nextChar = str.charAt(i + 1);

            switch (nextChar) {
                case '\\':
                    result.append('\\');
                    i++; // Skip the next character
                    break;
                case '"':
                    result.append('"');
                    i++; // Skip the next character
                    break;
                case '\'':
                    result.append('\'');
                    i++; // Skip the next character
                    break;
                case 'n':
                    result.append('\n');
                    i++; // Skip the next character
                    break;
                case 'r':
                    result.append('\r');
                    i++; // Skip the next character
                    break;
                case 't':
                    result.append('\t');
                    i++; // Skip the next character
                    break;
                case 'b':
                    result.append('\b');
                    i++; // Skip the next character
                    break;
                case 'f':
                    result.append('\f');
                    i++; // Skip the next character
                    break;
                case '/':
                    result.append('/');
                    i++; // Skip the next character
                    break;
                case 'u':
                    // Unicode escape sequence \XXXX
                    if (i + 5 < length) {
                        String unicode = str.substring(i + 2, i + 6);
                        try {
                            int unicodeValue = Integer.parseInt(unicode, 16);
                            result.append((char) unicodeValue);
                            i += 5; // Skip \XXXX
                        } catch (NumberFormatException e) {
                            // Invalid unicode sequence - treat as literal
                            result.append(ch);
                        }
                    } else {
                        // Incomplete unicode sequence - treat as literal
                        result.append(ch);
                    }
                    break;
                default:
                    // Check for octal escape sequence \XXX (1-3 digits)
                    if (nextChar >= '0' && nextChar <= '7') {
                        StringBuilder octal = new StringBuilder();
                        int j = i + 1;

                        // Collect up to 3 octal digits
                        while (j < length && j < i + 4 && str.charAt(j) >= '0' && str.charAt(j) <= '7') {
                            octal.append(str.charAt(j));
                            j++;
                        }

                        try {
                            int octalValue = Integer.parseInt(octal.toString(), 8);
                            if (octalValue <= 255) { // Valid character code
                                result.append((char) octalValue);
                                i = j - 1; // Skip the octal digits
                            } else {
                                // Invalid octal value - treat as literal
                                result.append(ch);
                            }
                        } catch (NumberFormatException e) {
                            // Invalid octal sequence - treat as literal
                            result.append(ch);
                        }
                    } else {
                        // Unknown escape sequence - keep the backslash
                        result.append(ch);
                    }
                    break;
            }
        }
        return result.toString();
    }
}
