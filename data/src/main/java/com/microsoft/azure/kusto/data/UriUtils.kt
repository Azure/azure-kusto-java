package com.microsoft.azure.kusto.data

import org.apache.hc.core5.net.InetAddressUtils
import org.apache.hc.core5.net.URIBuilder
import java.net.Inet4Address
import java.net.InetAddress
import java.net.URISyntaxException
import kotlin.io.path.Path
import kotlin.io.path.nameWithoutExtension


object UriUtils {
    @JvmStatic
    @Throws(URISyntaxException::class)
    @JvmOverloads
    fun setPathForUri(uri: String, path: String, ensureTrailingSlash: Boolean = false): String =
        URIBuilder(uri)
            .setPath(path)
            .toString()
            .apply { return if (ensureTrailingSlash && !endsWith("/")) "$this/" else this }

    @JvmStatic
    @Throws(URISyntaxException::class)
    fun appendPathToUri(uri: String, path: String): String = URIBuilder(uri).appendPath(path).toString()

    @JvmStatic
    fun isLocalAddress(host: String): Boolean {
        if (InetAddressUtils.isIPv4Address(host)) {
            return Inet4Address.getByName(host).isLoopbackAddress
        }
        if (InetAddressUtils.isIPv6Address(host)) {
            return InetAddress.getByName(host).isLoopbackAddress
        }
        return URIBuilder(host).let { it.host == "localhost" || it.host == null && it.path == "/localhost" }
    }

    @JvmStatic
    fun removeExtension(filename: String): String =
        Path(filename).let { it.resolveSibling(it.nameWithoutExtension) }.toString()

    @JvmStatic
    @Throws(URISyntaxException::class)
    fun getSasAndEndpointFromResourceURL(url: String): Array<String> = url.split('?').toTypedArray().let {
        if (it.size == 2) it else throw URISyntaxException(url, "URL does not contain a query string")
    }

    // Given a cmd line used to run the java app, this method strips out the file name running
    // i.e: "home/user/someFile.jar -arg1 val" -> someFile
    @JvmStatic
    fun stripFileNameFromCommandLine(cmdLine: String?): String? =
        cmdLine?.trim()?.substringBefore(' ')?.let { Path(it).fileName.toString() }
}
