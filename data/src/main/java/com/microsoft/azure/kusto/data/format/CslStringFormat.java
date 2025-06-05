package com.microsoft.azure.kusto.data.format;


import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.Utils;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CslStringFormat extends CslFormat {
    private static final Set<String> KUSTO_LITERAL_PREFIX = Stream.of("H", "h").collect(Collectors.toCollection(HashSet::new));
    private static final Set<String> KUSTO_MULTILINE_QUOTE_DELIMITERS = Stream.of("```", "~~~").collect(Collectors.toCollection(HashSet::new));
    private static final Set<String> KUSTO_ESCAPE_SEQUENCES = Stream.of("\\\"", "'", "@\\\"", "@'").collect(Collectors.toCollection(HashSet::new));

    private final String value;

    public CslStringFormat(String value) {
        this.value = value;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    String getValueAsString() {
        Ensure.stringIsNotBlank(value, "value");

        return value;
    }

    public static String parseStringLiteral(String value) {
        String result = value;
        if (KUSTO_LITERAL_PREFIX.contains(result.substring(0, 1))) {
            result = result.substring(1);
        }

        String multilineString = parseMultilineString(result);
        if (!Utils.isNullOrEmpty(multilineString)) {
            return multilineString;
        }

        return unescapeString(result);
    }

    private static String parseMultilineString(String quotedString) {
        for (String quoteDelimiter : KUSTO_MULTILINE_QUOTE_DELIMITERS) {
            if (quotedString.startsWith(quoteDelimiter)) {
                int twiceQuoteLen = quoteDelimiter.length() * 2;
                if (quotedString.length() >= twiceQuoteLen && quotedString.endsWith(quoteDelimiter)) {
                    return quotedString.substring(quoteDelimiter.length(), quotedString.length() - twiceQuoteLen);
                } else {
                    return quotedString.substring(quoteDelimiter.length());
                }
            }
        }

        return null;
    }

    private static String unescapeString(String escapedString) {
        for (String escapeSequence : KUSTO_ESCAPE_SEQUENCES) {
            if (escapedString.startsWith(escapeSequence)) {
                int escapeSequenceLength = escapeSequence.length() + 1;
                if (escapedString.length() >= escapeSequenceLength && escapedString.endsWith(escapeSequence)) {
                    String unescapedString = escapedString.substring(escapeSequence.length(), escapedString.length() - escapeSequence.length());
                    if ("\\\"".equals(escapeSequence) || "'".equals(escapeSequence)) {
                        return Utils.unescapeJava(unescapedString);
                    } else if (escapeSequence.startsWith("@")) {
                        String quote = escapeSequence.substring(1);
                        return escapedString.replaceAll(quote + quote, quote);
                    }
                }
            }
        }

        return escapedString;
    }
}
