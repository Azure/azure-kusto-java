package com.microsoft.azure.kusto.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KustoResultMapperTest {

    public static enum TestEnum {
        A, B, C
    }

    public static class TestPojo {
        int id;
        String name;
        ZonedDateTime zonedTime;
        Instant instantTime;
        TestEnum testEnum;
        UUID uuid;

        public void setId(int id) {
            this.id = id;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setZonedTime(ZonedDateTime time) {
            this.zonedTime = time;
        }

        public void setInstantTime(Instant time) {
            this.instantTime = time;
        }

        public void setUuid(UUID uuid) {
            this.uuid = uuid;
        }

        public void setTestEnum(TestEnum testEnum) {
            this.testEnum = testEnum;
        }
    }

    static final KustoResultMapper<TestPojo> namedMapper = KustoResultMapper.newBuilder(TestPojo::new)
            .addColumn(KustoType.INTEGER, "Id", false, TestPojo::setId)
            .addColumn(KustoType.STRING, "Name", true, TestPojo::setName)
            .addColumn(KustoType.DATETIME_ZONED_DATE_TIME, "TimeColumn", true, TestPojo::setZonedTime)
            .addColumn(KustoType.GUID_UUID, "Guid", true, TestPojo::setUuid)
            .addColumn(KustoType.STRING, "Enum", true, (r, t) -> r.setTestEnum(t != null ? TestEnum.valueOf(t) : null))
            .addColumn(KustoType.DATETIME_INSTANT, "Instant", true, TestPojo::setInstantTime).build();

    static final KustoResultMapper<TestPojo> ordinalMapper = KustoResultMapper.newBuilder(TestPojo::new)
            .addColumn(KustoType.INTEGER, 1, false, TestPojo::setId)
            .addColumn(KustoType.STRING, 2, true, TestPojo::setName)
            .addColumn(KustoType.DATETIME_ZONED_DATE_TIME, 3, true, TestPojo::setZonedTime)
            .addColumn(KustoType.GUID_UUID, 4, true, TestPojo::setUuid)
            .addColumn(KustoType.STRING, "Enum", true, (r, t) -> r.setTestEnum(t != null ? TestEnum.valueOf(t) : null))
            .addColumn(KustoType.DATETIME_INSTANT, 6, true, TestPojo::setInstantTime).build();

    static final KustoResultMapper<TestPojo> mixedMapper = KustoResultMapper.newBuilder(TestPojo::new)
            .addColumn(KustoType.INTEGER, "Id", 1, false, TestPojo::setId)
            .addColumn(KustoType.STRING, "Name", 2, true, TestPojo::setName)
            .addColumn(KustoType.DATETIME_ZONED_DATE_TIME, "TimeColumn", 3, true, TestPojo::setZonedTime)
            .addColumn(KustoType.GUID_UUID, "Guid", 4, true, TestPojo::setUuid)
            .addColumn(KustoType.STRING, "Enum", 5, true, (r, t) -> r.setTestEnum(t != null ? TestEnum.valueOf(t) : null))
            .addColumn(KustoType.DATETIME_INSTANT, "Instant", 6, true, TestPojo::setInstantTime).build();

    KustoResultSetTable resultSet = mock(KustoResultSetTable.class);

    @BeforeEach
    public void prepareResultSet() {
        when(this.resultSet.findColumn("Id")).thenReturn(1);
        when(this.resultSet.findColumn("Name")).thenReturn(2);
        when(this.resultSet.findColumn("TimeColumn")).thenReturn(3);
        when(this.resultSet.findColumn("Guid")).thenReturn(4);
        when(this.resultSet.findColumn("Enum")).thenReturn(5);
        when(this.resultSet.findColumn("Instant")).thenReturn(6);
    }

    void testSingleNonNull(KustoResultMapper<TestPojo> mapper) {
        when(this.resultSet.getObject(1)).thenReturn(1);
        when(this.resultSet.getObject(2)).thenReturn("MyName");
        when(this.resultSet.getObject(3)).thenReturn("1970-01-01T00:00:00.001Z");
        when(this.resultSet.getObject(4)).thenReturn("e091cf92-6195-4005-bad5-82af80ff1939");
        when(this.resultSet.getObject(5)).thenReturn("C");
        when(this.resultSet.getObject(6)).thenReturn("1970-01-01T00:00:00.002Z");
        when(this.resultSet.next()).thenReturn(true);

        TestPojo pojo = mapper.extractSingle(this.resultSet);
        assertNotNull(pojo);
        assertEquals(1, pojo.id);
        assertEquals("MyName", pojo.name);
        assertEquals(1, pojo.zonedTime.toInstant().toEpochMilli());
        assertEquals(UUID.fromString("e091cf92-6195-4005-bad5-82af80ff1939"), pojo.uuid);
        assertEquals(TestEnum.C, pojo.testEnum);
        assertEquals(2, pojo.instantTime.toEpochMilli());
    }

    void testSingleNullable(KustoResultMapper<TestPojo> mapper) {
        when(this.resultSet.getObject(1)).thenReturn(1);
        when(this.resultSet.getObject(2)).thenReturn(null);
        when(this.resultSet.getObject(3)).thenReturn(null);
        when(this.resultSet.getObject(4)).thenReturn(null);
        when(this.resultSet.getObject(5)).thenReturn(null);
        when(this.resultSet.getObject(6)).thenReturn(null);
        when(this.resultSet.next()).thenReturn(true);

        TestPojo pojo = mapper.extractSingle(this.resultSet);
        assertNotNull(pojo);
        assertEquals(1, pojo.id);
        assertNull(pojo.name);
        assertNull(pojo.zonedTime);
        assertNull(pojo.uuid);
        assertNull(pojo.testEnum);
        assertNull(pojo.instantTime);
    }

    void testSingleNullThrowing(KustoResultMapper<TestPojo> mapper) {
        when(this.resultSet.getObject(1)).thenReturn(null);
        when(this.resultSet.next()).thenReturn(true);

        assertThrows(NullPointerException.class, () -> mapper.extractSingle(this.resultSet));
    }

    void testList(KustoResultMapper<TestPojo> mapper) {
        when(this.resultSet.getObject(1)).thenReturn(1).thenReturn(2);
        when(this.resultSet.getObject(2)).thenReturn("MyName").thenReturn("OtherName");
        when(this.resultSet.getObject(3)).thenReturn("1970-01-01T00:00:00.001Z").thenReturn("1970-01-01T00:00:00.003Z");
        when(this.resultSet.getObject(4)).thenReturn("e091cf92-6195-4005-bad5-82af80ff1939").thenReturn("dc90cbef-0d82-4a79-bb34-7e7798bf962b");
        when(this.resultSet.getObject(5)).thenReturn("C").thenReturn("A");
        when(this.resultSet.getObject(6)).thenReturn("1970-01-01T00:00:00.002Z").thenReturn("1970-01-01T00:00:00.004Z");
        when(this.resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        List<TestPojo> list = mapper.extractList(this.resultSet);
        assertNotNull(list);
        assertEquals(2, list.size());

        TestPojo pojo = list.get(0);
        assertNotNull(pojo);
        assertEquals(1, pojo.id);
        assertEquals("MyName", pojo.name);
        assertEquals(1, pojo.zonedTime.toInstant().toEpochMilli());
        assertEquals(UUID.fromString("e091cf92-6195-4005-bad5-82af80ff1939"), pojo.uuid);
        assertEquals(TestEnum.C, pojo.testEnum);
        assertEquals(2, pojo.instantTime.toEpochMilli());

        pojo = list.get(1);
        assertNotNull(pojo);
        assertEquals(2, pojo.id);
        assertEquals("OtherName", pojo.name);
        assertEquals(3, pojo.zonedTime.toInstant().toEpochMilli());
        assertEquals(UUID.fromString("dc90cbef-0d82-4a79-bb34-7e7798bf962b"), pojo.uuid);
        assertEquals(TestEnum.A, pojo.testEnum);
        assertEquals(4, pojo.instantTime.toEpochMilli());
    }

    void testSingleNoRows(KustoResultMapper<TestPojo> mapper) {
        when(this.resultSet.next()).thenReturn(false);
        TestPojo pojo = mapper.extractSingle(this.resultSet);
        assertNull(pojo);
    }

    @Test
    void testExtractSingleOrdinalMapperNonNullable() {
        testSingleNonNull(ordinalMapper);
    }

    @Test
    void testExtractSingleMixedMapperNonNullable() {
        testSingleNonNull(mixedMapper);
    }

    @Test
    void testExtractSingleNamedMapperNonNullable() {
        testSingleNonNull(namedMapper);
    }

    @Test
    void testExtractSingleceOrdinalNoRows() {
        testSingleNoRows(ordinalMapper);
    }

    @Test
    void testExtractSingleMixedMapperNoRows() {
        testSingleNoRows(mixedMapper);
    }

    @Test
    void testExtractSingleNamedMapperNoRows() {
        testSingleNoRows(namedMapper);
    }

    @Test
    void testExtractListOrdinalMapperNonNullable() {
        testList(ordinalMapper);
    }

    @Test
    void testExtractListMixedMapperNonNullable() {
        testList(mixedMapper);
    }

    @Test
    void testExtractListNamedMapperNonNullable() {
        testList(namedMapper);
    }

    @Test
    void testExtractListOrdinalMapperNullable() {
        testSingleNullable(ordinalMapper);
    }

    @Test
    void testExtractListMixedMapperNullable() {
        testSingleNullable(mixedMapper);
    }

    @Test
    void testExtractListNamedMapperNullable() {
        testSingleNullable(namedMapper);
    }

    @Test
    void testExtractListOrdinalMapperNullThrowing() {
        testSingleNullThrowing(ordinalMapper);
    }

    @Test
    void testExtractListMixedMapperNullThrowing() {
        testSingleNullThrowing(mixedMapper);
    }

    @Test
    void testExtractListNamedMapperNullThrowing() {
        testSingleNullThrowing(namedMapper);
    }
}
