package com.microsoft.azure.kusto.ingest.utils;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamUtils {
    @NotNull
    public static <T> List<T> roundRobinNestedList(@NotNull List<List<T>> validResources) {
        int longestResourceList = validResources.stream().mapToInt(List::size).max().orElse(0);

        return IntStream.range(0, longestResourceList).boxed()
                .flatMap(i -> validResources.stream().map(r -> r.size() > i ? r.get(i) : null).filter(Objects::nonNull)).collect(Collectors.toList());
    }
}
