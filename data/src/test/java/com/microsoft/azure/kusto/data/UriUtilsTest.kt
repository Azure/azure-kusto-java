import com.microsoft.azure.kusto.data.UriUtils
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class UriUtilsTest {

    @Test
    fun setPathForUriAddsPathToUri() {
        val uri = "https://www.microsoft.com"
        val path = "/kusto"
        val result = UriUtils.setPathForUri(uri, path, false)
        assertEquals("https://www.microsoft.com/kusto", result)
    }

    @Test
    fun setPathForUriAddsPathAndTrailingSlashToUriWhenEnsureTrailingSlashIsTrue() {
        val uri = "https://www.microsoft.com"
        val path = "/kusto"
        val result = UriUtils.setPathForUri(uri, path, true)
        assertEquals("https://www.microsoft.com/kusto/", result)
    }

    @Test
    fun appendPathToUriAppendsPathToUri() {
        val uri = "https://www.microsoft.com"
        val path = "/kusto"
        val result = UriUtils.appendPathToUri(uri, path)
        assertEquals("https://www.microsoft.com/kusto", result)
    }

    @Test
    fun isLocalAddressReturnsTrueForLocalhost() {
        assertTrue(UriUtils.isLocalAddress("localhost"))
        assertTrue(UriUtils.isLocalAddress("http://localhost"))
    }


    @Test
    fun isLocalAddressReturnsTrueForLoopbackAddress() {
        val host = "127.0.0.1"
        val result = UriUtils.isLocalAddress(host)
        assertTrue(result)
    }

    @Test
    fun isLocalAddressReturnsFalseForNonLocalAddress() {
        val host = "www.microsoft.com"
        val result = UriUtils.isLocalAddress(host)
        assertFalse(result)
    }

    @Test
    fun removeExtensionRemovesFileExtension() {
        val filename = "file.txt"
        val result = UriUtils.removeExtension(filename)
        assertEquals("file", result)
    }

    @Test
    fun getSasAndEndpointFromResourceURLReturnsEndpointAndQuery() {
        val url = "https://www.microsoft.com/kusto?a=1&b=2"
        val result = UriUtils.getSasAndEndpointFromResourceURL(url)
        assertEquals("https://www.microsoft.com/kusto", result[0])
        assertEquals("a=1&b=2", result[1])
    }

    @Test
    fun stripFileNameFromCommandLineReturnsFileName() {
        val cmdLine = "home/user/someFile.jar -arg1 val"
        val result = UriUtils.stripFileNameFromCommandLine(cmdLine)
        assertEquals("someFile.jar", result)
    }

    @Test
    fun stripFileNameFromCommandLineReturnsNullForNullInput() {
        val cmdLine = null
        val result = UriUtils.stripFileNameFromCommandLine(cmdLine)
        assertNull(result)
    }
}
