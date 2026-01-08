// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.uploader

import com.microsoft.azure.kusto.ingest.v2.models.ContainerInfo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class UploaderModelsTest {

    // UploadMethod enum tests
    @Test
    fun `UploadMethod enum has correct number of values`() {
        assertEquals(3, UploadMethod.values().size)
    }

    @Test
    fun `UploadMethod DEFAULT exists`() {
        assertNotNull(UploadMethod.DEFAULT)
        assertEquals("DEFAULT", UploadMethod.DEFAULT.name)
    }

    @Test
    fun `UploadMethod STORAGE exists`() {
        assertNotNull(UploadMethod.STORAGE)
        assertEquals("STORAGE", UploadMethod.STORAGE.name)
    }

    @Test
    fun `UploadMethod LAKE exists`() {
        assertNotNull(UploadMethod.LAKE)
        assertEquals("LAKE", UploadMethod.LAKE.name)
    }

    @Test
    fun `UploadMethod valueOf works correctly`() {
        assertEquals(UploadMethod.DEFAULT, UploadMethod.valueOf("DEFAULT"))
        assertEquals(UploadMethod.STORAGE, UploadMethod.valueOf("STORAGE"))
        assertEquals(UploadMethod.LAKE, UploadMethod.valueOf("LAKE"))
    }

    @Test
    fun `UploadMethod values returns all enum constants`() {
        val values = UploadMethod.entries.toTypedArray()
        assertTrue(values.contains(UploadMethod.DEFAULT))
        assertTrue(values.contains(UploadMethod.STORAGE))
        assertTrue(values.contains(UploadMethod.LAKE))
    }

    // ExtendedContainerInfo tests
    @Test
    fun `ExtendedContainerInfo creates correctly with DEFAULT method`() {
        val containerInfo =
            ContainerInfo(
                path =
                "https://example.blob.core.windows.net/container?sv=2020-08-04&st=...",
            )
        val extended =
            ExtendedContainerInfo(containerInfo, UploadMethod.DEFAULT)

        assertEquals(containerInfo, extended.containerInfo)
        assertEquals(UploadMethod.DEFAULT, extended.uploadMethod)
    }

    @Test
    fun `ExtendedContainerInfo creates correctly with STORAGE method`() {
        val containerInfo =
            ContainerInfo(
                path =
                "https://storage.blob.core.windows.net/data?sv=2020-08-04&st=...",
            )
        val extended =
            ExtendedContainerInfo(containerInfo, UploadMethod.STORAGE)

        assertEquals(containerInfo, extended.containerInfo)
        assertEquals(UploadMethod.STORAGE, extended.uploadMethod)
    }

    @Test
    fun `ExtendedContainerInfo creates correctly with LAKE method`() {
        val containerInfo =
            ContainerInfo(
                path =
                "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse?sv=2020-08-04&st=...",
            )
        val extended = ExtendedContainerInfo(containerInfo, UploadMethod.LAKE)

        assertEquals(containerInfo, extended.containerInfo)
        assertEquals(UploadMethod.LAKE, extended.uploadMethod)
    }

    @Test
    fun `ExtendedContainerInfo data class equality works`() {
        val containerInfo1 =
            ContainerInfo("https://url1.blob.core.windows.net?token1")
        val containerInfo2 =
            ContainerInfo("https://url1.blob.core.windows.net?token1")
        val containerInfo3 =
            ContainerInfo("https://url2.blob.core.windows.net?token2")

        val extended1 =
            ExtendedContainerInfo(containerInfo1, UploadMethod.DEFAULT)
        val extended2 =
            ExtendedContainerInfo(containerInfo2, UploadMethod.DEFAULT)
        val extended3 =
            ExtendedContainerInfo(containerInfo3, UploadMethod.DEFAULT)
        val extended4 =
            ExtendedContainerInfo(containerInfo1, UploadMethod.STORAGE)

        assertEquals(extended1, extended2)
        assertNotEquals(extended1, extended3)
        assertNotEquals(extended1, extended4)
    }

    @Test
    fun `ExtendedContainerInfo data class hashCode works`() {
        val containerInfo =
            ContainerInfo("https://url.blob.core.windows.net?token")
        val extended1 =
            ExtendedContainerInfo(containerInfo, UploadMethod.DEFAULT)
        val extended2 =
            ExtendedContainerInfo(containerInfo, UploadMethod.DEFAULT)

        assertEquals(extended1.hashCode(), extended2.hashCode())
    }

    @Test
    fun `ExtendedContainerInfo data class copy works`() {
        val containerInfo =
            ContainerInfo("https://url.blob.core.windows.net?token")
        val original =
            ExtendedContainerInfo(containerInfo, UploadMethod.DEFAULT)
        val copied = original.copy(uploadMethod = UploadMethod.LAKE)

        assertEquals(original.containerInfo, copied.containerInfo)
        assertEquals(UploadMethod.DEFAULT, original.uploadMethod)
        assertEquals(UploadMethod.LAKE, copied.uploadMethod)
    }

    @Test
    fun `ExtendedContainerInfo copy can change containerInfo`() {
        val containerInfo1 =
            ContainerInfo("https://url1.blob.core.windows.net?token1")
        val containerInfo2 =
            ContainerInfo("https://url2.blob.core.windows.net?token2")
        val original =
            ExtendedContainerInfo(containerInfo1, UploadMethod.DEFAULT)
        val copied = original.copy(containerInfo = containerInfo2)

        assertEquals(containerInfo2, copied.containerInfo)
        assertEquals(original.uploadMethod, copied.uploadMethod)
    }

    @Test
    fun `ExtendedContainerInfo toString contains all fields`() {
        val containerInfo =
            ContainerInfo("https://url.blob.core.windows.net?token")
        val extended =
            ExtendedContainerInfo(containerInfo, UploadMethod.STORAGE)

        val stringRep = extended.toString()
        assertTrue(stringRep.contains("containerInfo"))
        assertTrue(stringRep.contains("uploadMethod"))
    }

    @Test
    fun `ExtendedContainerInfo component functions work`() {
        val containerInfo =
            ContainerInfo("https://url.blob.core.windows.net?token")
        val extended = ExtendedContainerInfo(containerInfo, UploadMethod.LAKE)

        val (info, method) = extended

        assertEquals(containerInfo, info)
        assertEquals(UploadMethod.LAKE, method)
    }

    @Test
    fun `ExtendedContainerInfo works with different upload methods`() {
        val containerInfo =
            ContainerInfo("https://url.blob.core.windows.net?token")

        val default = ExtendedContainerInfo(containerInfo, UploadMethod.DEFAULT)
        val storage = ExtendedContainerInfo(containerInfo, UploadMethod.STORAGE)
        val lake = ExtendedContainerInfo(containerInfo, UploadMethod.LAKE)

        assertNotEquals(default, storage)
        assertNotEquals(storage, lake)
        assertNotEquals(default, lake)
    }

    @Test
    fun `ExtendedContainerInfo handles empty SAS token`() {
        val containerInfo = ContainerInfo("https://url.blob.core.windows.net")
        val extended =
            ExtendedContainerInfo(containerInfo, UploadMethod.DEFAULT)

        assertNotNull(extended.containerInfo.path)
    }
}
