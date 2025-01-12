package com.microsoft.azure.kusto.data.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class KcsbKeywords {
    public KeywordData[] keywords;

    private final Map<String, KnownKeywords> lookup = new HashMap<>();

    private static KcsbKeywords instance = null;
    private static final Object object = new Object();

    public static KcsbKeywords getInstance() {
        if (instance != null)
            return instance;
        synchronized (object) {
            if (instance == null) {
                instance = readInstance();
            }
        }
        return instance;
    }

    // For Deserialization
    public KcsbKeywords() {
    }

    public static String normalize(String keyword) {
        return keyword.toLowerCase().replace(" ", "");
    }

    @NotNull
    private static KcsbKeywords readInstance() {
        try {
            ObjectMapper objectMapper = Utils.getObjectMapper();
            try (InputStream resourceAsStream = KcsbKeywords.class.getResourceAsStream(
                    "/kcsb.json")) {
                KcsbKeywords value = objectMapper.readValue(resourceAsStream, KcsbKeywords.class);

                // Validate the keywords
                for (KeywordData keywordData : value.keywords) {
                    KnownKeywords keyword = KnownKeywords.knownKeywords.get(keywordData.name);
                    keyword.setType(keywordData.type);
                    keyword.setSecret(keywordData.secret);

                    value.lookup.put(normalize(keywordData.name), keyword);

                    for (String alias : keywordData.aliases) {
                        if (value.lookup.containsKey(alias)) {
                            throw new RuntimeException("KCSB keywordMap alias is duplicated: `" + alias + "`");
                        }
                        value.lookup.put(normalize(alias), keyword);
                    }
                }

                return value;
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to read kcsb.json", ex);
        }
    }

    public KnownKeywords get(String keyword) {
        KnownKeywords result = lookup.get(normalize(keyword));
        if (result == null) {
            throw new IllegalArgumentException("Keyword `" + keyword + "` is not a known keyword");
        }

        if (!result.isSupported()) {
            throw new IllegalArgumentException("Keyword `" + keyword + "` is not supported");
        }

        return result;
    }
}
