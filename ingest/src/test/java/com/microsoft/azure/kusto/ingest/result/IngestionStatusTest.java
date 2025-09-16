package com.microsoft.azure.kusto.ingest.result;

import com.azure.data.tables.models.TableEntity;
import com.microsoft.azure.kusto.data.StringUtils;

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