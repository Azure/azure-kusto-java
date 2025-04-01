package com.microsoft.azure.kusto.data.exceptions;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KustoServiceQueryErrorTest {
    @Test
    void testConstructorWithMessageAndExceptions() {
        List<RuntimeException> exceptions = new ArrayList<>();
        exceptions.add(new RuntimeException("Test exception"));
        KustoServiceQueryError error = new KustoServiceQueryError("Test message", exceptions);

        assertEquals("Test message", error.getMessage());
        assertEquals(exceptions, error.getExceptions());
        assertEquals(1, error.getExceptions().size());
        assertEquals("Test exception", error.getExceptions().get(0).getMessage());
    }

    @Test
    void testConstructorWithMessage() {
        KustoServiceQueryError error = new KustoServiceQueryError("Test message");

        assertEquals("Test message", error.getMessage());
        assertEquals(1, error.getExceptions().size());
        assertEquals("Test message", error.getExceptions().get(0).getMessage());
    }

    @Test
    void testFromOneApiErrorArrayWithMultipleExceptions() {
        ArrayNode jsonExceptions = JsonNodeFactory.instance.arrayNode();
        jsonExceptions.add("Exception 1");
        jsonExceptions.add("Exception 2");

        KustoServiceQueryError error = KustoServiceQueryError.fromOneApiErrorArray(jsonExceptions, false);

        assertEquals("Query execution failed with multiple inner exceptions:\nException 1\nException 2\n", error.getMessage());
        assertEquals(2, error.getExceptions().size());
        assertEquals("Exception 1", error.getExceptions().get(0).getMessage());
        assertEquals("Exception 2", error.getExceptions().get(1).getMessage());
    }

    @Test
    void testFromOneApiErrorArrayWithSingleException() {
        ArrayNode jsonExceptions = JsonNodeFactory.instance.arrayNode();
        jsonExceptions.add("Single exception");

        KustoServiceQueryError error = KustoServiceQueryError.fromOneApiErrorArray(jsonExceptions, false);

        assertEquals("Single exception\n", error.getMessage());
        assertEquals(1, error.getExceptions().size());
        assertEquals("Single exception", error.getExceptions().get(0).getMessage());
    }

    @Test
    void testFromOneApiErrorArrayWithOneApi() {
        ArrayNode jsonExceptions = JsonNodeFactory.instance.arrayNode();
        jsonExceptions.add(new TextNode("API exception"));

        KustoServiceQueryError error = KustoServiceQueryError.fromOneApiErrorArray(jsonExceptions, true);

        assertEquals("API exception\n", error.getMessage());
        assertEquals(1, error.getExceptions().size());
        assertInstanceOf(DataWebException.class, error.getExceptions().get(0));
    }

    @Test
    void testToString() {
        KustoServiceQueryError error = new KustoServiceQueryError("Test message");
        assertTrue(error.toString().contains("exceptions\":[java.lang.RuntimeException: Test message]}"));

        KustoServiceQueryError emptyError = new KustoServiceQueryError("Empty message");
        emptyError.getExceptions().clear();
        assertEquals("Empty message", emptyError.toString());
    }

    @Test
    void testIsPermanentWithRuntimeException() {
        KustoServiceQueryError error = new KustoServiceQueryError("Test message");
        assertFalse(error.isPermanent());
    }

    @Test
    void testIsPermanentWithEmptyExceptions() {
        List<RuntimeException> emptyExceptions = new ArrayList<>();
        KustoServiceQueryError error = new KustoServiceQueryError("Test message", emptyExceptions);
        assertFalse(error.isPermanent());
    }
}
