package com.microsoft.azure.kusto.data;

import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.concurrent.TimeUnit;

public class ClientRequestPropertiesTest {
    @Test
    @DisplayName("test set/get timeout")
    void testSetGetTimeout() {
        ClientRequestProperties props = new ClientRequestProperties();
        Long expected = TimeUnit.MINUTES.toMillis(100);

        // before setting value should be null
        Assertions.assertEquals(null, props.getTimeoutInMilliSec());

        props.setTimeoutInMilliSec(expected);
        Assertions.assertEquals(expected, props.getTimeoutInMilliSec());
    }

    @Test
    @DisplayName("test ClientRequestProperties toString")
    void testToString() throws JSONException {
        ClientRequestProperties props = new ClientRequestProperties();
        props.setOption("a", 1);
        props.setOption("b", "hello");

        JSONAssert.assertEquals("{\"Options\": {\"a\":1, \"b\":\"hello\"}}", props.toString(), false);
    }
}
