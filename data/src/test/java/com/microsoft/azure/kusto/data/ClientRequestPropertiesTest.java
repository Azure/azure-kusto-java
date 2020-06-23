// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
    void timeoutSetGet() {
        ClientRequestProperties props = new ClientRequestProperties();
        Long expected = TimeUnit.MINUTES.toMillis(100);

        // before setting value should be null
        Assertions.assertNull(props.getTimeoutInMilliSec());

        props.setTimeoutInMilliSec(expected);
        Assertions.assertEquals(expected, props.getTimeoutInMilliSec());
    }

    @Test
    @DisplayName("test ClientRequestProperties toString")
    void propertiesToString() throws JSONException {
        ClientRequestProperties props = new ClientRequestProperties();
        props.setOption("a", 1);
        props.setOption("b", "hello");

        JSONAssert.assertEquals("{\"Options\": {\"a\":1, \"b\":\"hello\"}}", props.toString(), false);
    }

    @Test
    @DisplayName("test ClientRequestProperties fromString")
    void stringToProperties() throws JSONException {
        String properties = "{\"Options\":{\"servertimeout\":100000, \"Content-Encoding\":\"gzip\"},\"Parameters\":{\"birthday\":\"datetime(1970-05-11)\",\"courses\":\"dynamic(['Java', 'C++'])\"}}";
        ClientRequestProperties crp = ClientRequestProperties.fromString(properties);

        assert crp.getTimeoutInMilliSec() != null;
        assert crp.getOption("Content-Encoding").equals("gzip");
        assert crp.getParameter("birthday").equals("datetime(1970-05-11)");
        assert crp.getParameter("courses").equals("dynamic(['Java', 'C++'])");
    }
}
