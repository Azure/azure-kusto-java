package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.models.TableEntity;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class IngestionStatusTest {
    static Stream<Object[]> fromIdTestCases() {
        UUID uuid = UUID.randomUUID();
        return Stream.of(
                //happy path
                new Object[]{uuid.toString(), uuid},
                new Object[]{uuid, uuid},
                //edge cases
                new Object[]{"", null},
                new Object[]{null, null},
                new Object[]{"not-a-uuid", null}
        );
    }

    @ParameterizedTest
    @MethodSource("fromIdTestCases")
    void testFromId(Object input, UUID expected) {
        UUID result = IngestionStatus.fromId(input);
        Assertions.assertEquals(expected, result);
    }
}