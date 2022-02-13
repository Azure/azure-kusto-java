// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.source;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.ResultSet;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ResultSetSourceInfoTest {

    @Test
    @DisplayName("test set/get resultset")
    void ResultSet_GetSet_Success() {
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);
        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(mockResultSet);

        // this also tests that the constructor worked as expected
        Assertions.assertEquals(mockResultSet, resultSetSourceInfo.getResultSet());

        // use the setter to replace the resultset
        ResultSet mockResultSet1 = Mockito.mock(ResultSet.class);
        resultSetSourceInfo.setResultSet(mockResultSet1);
        Assertions.assertEquals(mockResultSet1, resultSetSourceInfo.getResultSet());
    }

    @Test
    @DisplayName("test new object with null resultset")
    void Constructor_NullResultSet_Exception() {
        assertThrows(NullPointerException.class, () -> new ResultSetSourceInfo(null));
    }

    @Test
    @DisplayName("test object toString")
    void toString_Success() {
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);
        UUID uuid = UUID.randomUUID();

        ResultSetSourceInfo resultSetSourceInfo = new ResultSetSourceInfo(mockResultSet, uuid);

        Assertions.assertTrue(resultSetSourceInfo.toString().contains(uuid.toString()));
    }
}
